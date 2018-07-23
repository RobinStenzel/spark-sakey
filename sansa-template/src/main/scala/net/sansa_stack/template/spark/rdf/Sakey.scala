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

//import scalax.collection.Graph 
import scalax.collection.mutable.Graph
import scalax.collection.GraphPredef._, scalax.collection.GraphEdge._, scalax.collection._
import scala.collection.Set
import shapeless.LowPriority.For
import scala.collection.immutable.HashSet


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
  
  //TEST MIN FILL
  val testSetEdges = mutable.HashSet.empty[UnDiEdge[VertexId]]
  val testSetNodes = mutable.HashSet.empty[VertexId]
  val newId = 1L
  val newId2 = 2L
  val newId3 = 3L
  val newId4 = 4L
  val newId5 = 5L
  val newId6 = 6L
  val newId7 = 7L
  /*testSetNodes.+=(newId)
  testSetNodes.+=(newId2)
  testSetNodes.+=(newId3)
  testSetNodes.+=(newId4)
  testSetNodes.+=(newId5)
  testSetNodes.+=(newId6)*/
  val newEdge = UnDiEdge(newId, newId2)
  val newEdge2 = UnDiEdge(newId2, newId3)
  val newEdge3 = UnDiEdge(newId, newId3)

  //val newEdge4 = UnDiEdge(newId3, newId)
  //val newEdge5 = UnDiEdge(newId5, newId)
  //val newEdge6 = UnDiEdge(newId6, newId)
  //val newEdge7 = UnDiEdge(newId4, newId)
  

  testSetEdges.+= (newEdge)
  testSetEdges.+= (newEdge2)
  testSetEdges.+= (newEdge3)
  //testSetEdges.+= (newEdge4)
  //testSetEdges.+= (newEdge5)
  //testSetEdges.+= (newEdge6)
  //testSetEdges.+= (newEdge7)
  //testSetEdges.+= (newEdge8)
  
  
  val g2 = Graph.from(testSetNodes, testSetEdges)
  val test = g2.edges
  
  
  //returns min-fill: number of edges needed to be filled to fully connect the node's parents
  def getNodeFill (node: g2.NodeT) : Int = {
    node.neighbors
      .flatMap(x => node.neighbors.map(y => (x, y)))
      .filter(f => f._1 < f._2 && !g2.edges.contains(UnDiEdge(f._1,f._2))).size
  }

  //ordering of Nodes based on min-fill
  object NodeOrderingFill extends Ordering[g2.NodeT]{
    def compare(x: g2.NodeT, y: g2.NodeT): Int = getNodeFill(x) compare getNodeFill(y)
  }
  
  //creates the min-fill induced graph (chordal graph)
  def minFillElimination (): Graph[VertexId,UnDiEdge] = {
     val sizeGraph = g2.nodes.size
     val newGraph = Graph.empty[VertexId,UnDiEdge]
     newGraph ++= g2
     var x = 0
     for ( x  <- 1 to sizeGraph){ 
       val minFillNode = g2.nodes.min(NodeOrderingFill)
       println(minFillNode)
       val newEdges =  minFillNode.neighbors
                       .flatMap(x => minFillNode.neighbors.map(y => UnDiEdge(x.value, y.value)))
                       .filter(f => !(f._1 == f._2) && !g2.edges.contains(UnDiEdge(f._1,f._2)))
       println(newEdges)
       g2 ++= newEdges
       newGraph ++= newEdges
       g2 -=  minFillNode
       
     }
     return newGraph
  }  
  
  def getNodeCardinality(node: g3.NodeT): Int = {
    node.neighbors
      .filter(f => g3.contains(f)).size
  }
  
  object NodeOrderingCardinality extends Ordering[g3.NodeT]{
    def compare(x: g3.NodeT, y: g3.NodeT): Int = getNodeCardinality(x) compare getNodeCardinality(y)
  }
  
  def maxCardinalityList(): mutable.HashSet[mutable.HashSet[VertexId]] ={
    val sizeGraph = g3.nodes.size
    val listRemovedNodes = mutable.HashSet.empty[VertexId]
    val setOfCliques = mutable.HashSet.empty[mutable.HashSet[VertexId]]
    var x = 0
    for ( x  <- 1 to sizeGraph){ 
      val maxCardinalityNode = g3.nodes.max(new Ordering[g3.NodeT] {
        def compare(x: g3.NodeT, y: g3.NodeT): Int = x.neighbors
          .filter(f => listRemovedNodes.contains(f.value) && !listRemovedNodes.contains(x.value)).size compare y.neighbors
          .filter(f => listRemovedNodes.contains(f.value) && !listRemovedNodes.contains(y.value)).size
      })
      println(s"Hello, $maxCardinalityNode")
      val neighbors = maxCardinalityNode.neighbors
      println(s"Neighbors, $neighbors")
      println(maxCardinalityNode.neighbors
      .filter(f => listRemovedNodes.contains(f)).size)
      
      val clique = mutable.HashSet.empty[VertexId]
      clique.+=(maxCardinalityNode.value)
      clique ++= maxCardinalityNode.neighbors.map(_.value).filter(f => listRemovedNodes.contains(f))
      println(s"Cliques, $setOfCliques")
      
      setOfCliques += clique  
      
      listRemovedNodes += maxCardinalityNode.value
      println(listRemovedNodes)
        
    }
    setOfCliques.filter(f => setOfCliques.forall(p => !f.subsetOf(p) || p == f))
  }
  println(g2)
  val g3 = minFillElimination()
  println(g3)
  val set1 = maxCardinalityList()
  println(set1)
  
      
  
  

}
  
  
 