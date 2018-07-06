package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._
import scala.collection.mutable
import org.apache.spark.graphx.Graph
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model.graph._

import scala.collection.immutable.HashSet
import org.apache.spark.rdd.RDD
import org.apache.jena.graph._


object Sakey extends App{
  
  
  //changes triples into (property, set of sets of subjects)
  def getFinalMap (triples: RDD[Triple]): (RDD[(Node, mutable.HashSet[mutable.HashSet[Node]])], Array[Node]) = {

    val predObjMapping = triples.map { s => ((s.getPredicate, s.getObject), s.getSubject )}
    val initialSet = mutable.HashSet.empty[Node]
    val addToSet = (s: mutable.HashSet[Node], v: Node) => s += v
    val mergePartitionSets = (p1: mutable.HashSet[Node], p2: mutable.HashSet[Node])=> p1 ++= p2
    val predObjAggregation = predObjMapping.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
    
    val predMapping = predObjAggregation.map {  case ((predicate, objects), subject) => (predicate, (objects, subject))}
    val initialSetofSets = mutable.HashSet.empty[mutable.HashSet[Node]]
    
    /*
     * ignore sets of size 1 (Singleton sets filtering)
     * check if new set is already included in subsets
     * if no, remove subsets of new set in first set and add new set 
     * (v-exception set filtering)
     */
    val addToSetofSets = {(s: mutable.HashSet[mutable.HashSet[Node]], v:  (Node,mutable.HashSet[Node])) => 
      if(v._2.size > 1 && !s.subsets().contains(v._2)) s.filter(!_.subsetOf(v._2)) += v._2 else s                    
    }
    
    /*
     * before appending sets, remove all subsets of elements of the opposite set
     */
    val mergePartitionSetsofSets = {(p1: mutable.HashSet[mutable.HashSet[Node]], p2: mutable.HashSet[mutable.HashSet[Node]]) =>
      p1.filter(p1 => p2.forall(x => !p1.subsetOf(x))) ++= p2.filter(p2 => p1.forall(x => !p2.subsetOf(x)))
    }      

    val predAggregation = predMapping.aggregateByKey(initialSetofSets)(addToSetofSets, mergePartitionSetsofSets)
    
    //temporarely disabled since an alternative is used to collect almostkeys
    //select all empty predicate aggregation sets and add their predicate to almost keys
    //predAggregation.collect().foreach(f => if (f._2.isEmpty) almostKeys.+(f._1))
    
    //remove empty sets 
    val finalMap = predAggregation.filter(!_._2.isEmpty) 
    
    //Array of properties with empty sets
    val almostKeys = predMapping.keys.distinct().subtract(finalMap.keys).collect()

    return ( finalMap, almostKeys)
  
  }
  
  
  val spark = SparkSession.builder
  .master("local[*]")
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .getOrCreate()
  
  //Sansa read in triples
  val input = "src/main/resources/rdf.nt"
  val lang = Lang.RDFXML
  val triples = spark.rdf(lang)(input)
  
  triples.cache()


  val finalMap = getFinalMap(triples)._1
  
  //flatten Set of Sets to one single Set
  val flatMap = finalMap.flatMapValues(identity).reduceByKey((x,y) => x.++(y))
  
  //Create all combinations of (property, set of subjects, property2, set2 of subjects)
  //remove every combination with intersections set lower than n
  val filteredQuadruple = flatMap.cartesian(flatMap)
  .filter(f => f._1._1 != f._2._1 && f._1._2.intersect(f._2._2).size >= 2 && f._1._1.hashCode() < f._2._1.hashCode())
  
  //create Graph out of quadruple
  val edges = filteredQuadruple.map { s => (s._1._1, s._1._1, s._2._1)}
  val tripleRDD = edges.map(f => Triple.create(f._1, f._2, f._3))  
  val graph = tripleRDD.asGraph()

  //collect all properties that don't appear in any edge and put them in the list of non keys
  val nNonKeys= finalMap.keys.distinct().subtract(graph.vertices.values).collect()
  nNonKeys.foreach(println)
  

}
  
  
 