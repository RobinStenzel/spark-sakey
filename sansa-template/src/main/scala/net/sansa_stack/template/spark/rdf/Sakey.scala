package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._

import scala.collection.mutable
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.EdgeDirection
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model.graph._

import org.apache.spark.rdd.RDD
import org.apache.jena.graph._
import org.apache.spark.graphx.EdgeTriplet

import scalax.collection.Graph // or scalax.collection.mutable.Graph
import scalax.collection.GraphPredef._, scalax.collection.GraphEdge._, scalax.collection._
import scala.collection.Set
import shapeless.LowPriority.For


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
  //val input = "src/main/resources/graph.nt"
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
  
  
  
  //calculate CCs with GraphX
  val connectedComponents = graph.connectedComponents()
  
  val mappedCC = connectedComponents.triplets.map(_.toTuple).map{ case ((v1,v2), (v3,v4), n1) => (v2, UnDiEdge(v1,v3))}
  
  //aggregate edges of the same connected component
  val edgeSet = mutable.HashSet.empty[UnDiEdge[VertexId]]
  val edgeSeqOp = (s: mutable.HashSet[UnDiEdge[VertexId]], v: UnDiEdge[VertexId]) => s += v
  val edgeCombOp = (p1: mutable.HashSet[UnDiEdge[VertexId]], p2: mutable.HashSet[UnDiEdge[VertexId]])=> p1 ++= p2
  
  //aggregate vertices of the same connected component
  val vertexSet = mutable.HashSet.empty[VertexId]
  val vertexSeq = (s: mutable.HashSet[VertexId], v: VertexId) => s += v
  val vertexCombOp = (p1: mutable.HashSet[VertexId], p2: mutable.HashSet[VertexId])=> p1 ++= p2
  
  //swap vertexId with assigned CC and do the aggregation
  val vert = connectedComponents.vertices.map({case (v1,v2) => (v2,v1)}).aggregateByKey(vertexSet)(vertexSeq, vertexCombOp)
  
  //join Edges and vertices with the same CC
  val joinedCC = mappedCC.aggregateByKey(edgeSet)(edgeSeqOp, edgeCombOp).join(vert)
  
  //create scalax graph out of edge and vertex set (better suited for removing and adding edges)
  val graphCC = joinedCC.map(f => Graph.from(f._2._2, f._2._1))
  
  val testSetEdges = mutable.HashSet.empty[UnDiEdge[VertexId]]
  val testSetNodes = mutable.HashSet.empty[VertexId]
  val newId = 1L
  val newId2 = 2L
  val newId3 = 3L
  val newId4 = 4L
  val newId5 = 5L
  val newId6 = 6L
  testSetNodes.+=(newId)
  testSetNodes.+=(newId2)
  testSetNodes.+=(newId3)
  testSetNodes.+=(newId4)
  testSetNodes.+=(newId5)
  testSetNodes.+=(newId6)
  val newEdge = UnDiEdge(newId, newId2)
  val newEdge2 = UnDiEdge(newId, newId3)
  val newEdge3 = UnDiEdge(newId, newId5)
  val newEdge4 = UnDiEdge(newId2, newId4)
  val newEdge5 = UnDiEdge(newId2, newId5)
  val newEdge6 = UnDiEdge(newId3, newId4)
  val newEdge7 = UnDiEdge(newId4, newId6)

  testSetEdges.+= (newEdge)
  testSetEdges.+= (newEdge2)
  testSetEdges.+= (newEdge3)
  testSetEdges.+= (newEdge4)
  testSetEdges.+= (newEdge5)
  testSetEdges.+= (newEdge6)
  testSetEdges.+= (newEdge7)
  val g2 = Graph.from(testSetNodes, testSetEdges)
  val nodes = g2.nodes
  def getNodeFill (node: g2.NodeT) : Int = {
    node.neighbors.zip(node.neighbors).filter(f => f._1 < f._2 && !g2.edges.contains(UnDiEdge(f._1,f._2))).size
  }
  nodes.foreach({node => node.neighbors.zip(node.neighbors).filter(f => f._1 < f._2 && !g2.edges.contains(UnDiEdge(f._1,f._2))).size
    
  })
  println(g2)
  
  

}
  
  
 