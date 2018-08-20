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

import scala.util.control.Breaks._


object Sakey extends App{
  

  val THIS_IS_VALUE_OF_N = 3
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
  /*
  val input = "src/main/resources/page_links_simple.nt"
  val lang = Lang.NTRIPLES
  val triples = spark.rdf(lang)(input)
  triples.collect().foreach(println)
  */
  
  val input = "src/main/resources/rdf.nt"
  val lang = Lang.RDFXML
  val triples = spark.rdf(lang)(input)
  

  triples.cache()

  

  var t0 = System.nanoTime()
  val finalMapTuple = getFinalMap(triples)
  val finalMap = finalMapTuple._1
  var t1 = System.nanoTime()
  println("Elapsed time: " + (t1 - t0)/1000000000 + "s cp1: FinalMap")
  
  
  t0 = System.nanoTime()
  //flatten Set of Sets to one single Set
  val flatMap = finalMap.flatMapValues(identity).reduceByKey((x,y) => x.++(y))
  

  
  //Create all combinations of (property, set of subjects, property2, set2 of subjects)
  //remove every combination with intersections set lower than n
  val filteredQuadruple = flatMap.cartesian(flatMap)
  .filter(f => f._1._1 != f._2._1 && f._1._2.intersect(f._2._2).size > THIS_IS_VALUE_OF_N && f._1._1.hashCode() < f._2._1.hashCode())
  
  t1 = System.nanoTime()
  println("Elapsed time: " + (t1 - t0)/1000000000 + "s cp2: flattenedMap")


  t0 = System.nanoTime()
  //create Graph out of quadruple
  val edges = filteredQuadruple.map { s => (s._1._1, s._1._1, s._2._1)}
  val tripleRDD = edges.map(f => Triple.create(f._1, f._2, f._3))  
  val graph = tripleRDD.asGraph()
  
  val mapVertexidToValue = mutable.HashMap.empty[VertexId, Node]
  graph.vertices.foreach(f => mapVertexidToValue.+=(f._1 -> f._2))


  //collect all properties that don't appear in any edge and put them in the list of non keys
  val isolatedNNonKeys= finalMap.keys.distinct().subtract(graph.vertices.values).collect()
  t1 = System.nanoTime()
  println("Elapsed time: " + (t1 - t0)/1000000000 + "s cp3: GraphX Graph")

  

  t0 = System.nanoTime()
  //calculate CCs with GraphX
  val connectedComponents = graph.connectedComponents()
  val mappedCC = connectedComponents.triplets.map(_.toTuple).map{ case ((v1,v2), (v3,v4), n1) => (v2, UnDiEdge(v1,v3))}
  t1 = System.nanoTime()
  println("Elapsed time: " + (t1 - t0)/1000000000 + "s cp4: ConnecteComponents")  

  t0 = System.nanoTime()
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
  joinedCC.take(100).foreach(println)
  
  t1 = System.nanoTime()
  println("Elapsed time: " + (t1 - t0)/1000000000 + "s cp5: Scalax Graph")
  
  t0 = System.nanoTime()
  //create scalax graph out of edge and vertex set (better suited for removing and adding edges)
  val graphCC = joinedCC.map(f 
      =>{
        val nodes = f._2._2
        val edges = f._2._1
        getMaxCliques(nodes, edges)
      })
  
  graphCC.take(50).foreach(println)
  
    t1 = System.nanoTime()
  println("Elapsed time: " + (t1 - t0)/1000000000 + "s cp6: MaxCliques")
  
  t0 = System.nanoTime()
  val nonKeysRDD = graphCC.map(f =>{
        getNonKeys(f._1, THIS_IS_VALUE_OF_N, f._2)
      })
  
  val emptyNonKeys = mutable.HashSet.empty[mutable.HashSet[Node]]
  val nonKeysSeqOp = (s: mutable.HashSet[mutable.HashSet[Node]], v: mutable.HashSet[mutable.HashSet[Node]]) => s ++= v
  val nonKeysCombOp = (p1: mutable.HashSet[mutable.HashSet[Node]], p2: mutable.HashSet[mutable.HashSet[Node]])=> p1 ++= p2
  val aggregatedNonKeys = nonKeysRDD.treeAggregate(emptyNonKeys)(nonKeysSeqOp, nonKeysCombOp)
  
  //combine nNonKeys with the ones found in PNK creation
  isolatedNNonKeys.foreach(f => aggregatedNonKeys += mutable.HashSet(f))
  aggregatedNonKeys.take(50).foreach(println)
  
  t1 = System.nanoTime()
  println("Elapsed time: " + (t1 - t0)/1000000000 + "s cp7: NonKeyFinder") 
  
  t0 = System.nanoTime()  
  val complement = getComplement(aggregatedNonKeys)
  
  //combine almostKEys with the ones found in finalMap creation
  val almostKeys = keyDerivation(complement) 
  finalMapTuple._2.foreach(f => almostKeys += mutable.HashSet(f))
  println(almostKeys)
  t1 = System.nanoTime()
  println("Elapsed time: " + (t1 - t0)/1000000000 + "s cp8: AlmostKeys")
  
 
  //uses minFill to create a chordal graph and maxCardinanilityOrdering to extract cliques
  //returns one pair of (set of cliques, distinct nodes) per connected component 
  def getMaxCliques (nodes: mutable.HashSet[VertexId], edges: mutable.HashSet[GraphEdge.UnDiEdge[VertexId]]) :  (mutable.HashSet[mutable.HashSet[Node]], mutable.HashSet[Node]) = {
    val g2 = Graph.from(nodes, edges)
    
    
    
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
         val newEdges =  minFillNode.neighbors
                         .flatMap(x => minFillNode.neighbors.map(y => UnDiEdge(x.value, y.value)))
                         .filter(f => !(f._1 == f._2) && !g2.edges.contains(UnDiEdge(f._1,f._2)))
         g2 ++= newEdges
         newGraph ++= newEdges
         g2 -=  minFillNode
         
       }
       return newGraph
    } 
    
    //create chordal graph using min-fill
    val g3 = minFillElimination()
    
    //counts the number of already ordered(removed) nodes it is connected to
    def getNodeCardinality(node: g3.NodeT): Int = {
      node.neighbors
        .filter(f => g3.contains(f)).size
    }
    
    //ordering based on maxCardinality
    object NodeOrderingCardinality extends Ordering[g3.NodeT]{
      def compare(x: g3.NodeT, y: g3.NodeT): Int = getNodeCardinality(x) compare getNodeCardinality(y)
    }
    
    //returns set of MaxCliques and set of distinct Nodes
    def maxCardinalityList(): (mutable.HashSet[mutable.HashSet[Node]], mutable.HashSet[Node]) ={
      val sizeGraph = g3.nodes.size
      val listRemovedNodes = mutable.HashSet.empty[VertexId]
      val setOfCliques = mutable.HashSet.empty[mutable.HashSet[Node]]
      val nodesOfCC = mutable.HashSet.empty[Node]
      var x = 0
      for ( x  <- 1 to sizeGraph){ 
        val maxCardinalityNode = g3.nodes.max(new Ordering[g3.NodeT] {
          def compare(x: g3.NodeT, y: g3.NodeT): Int = x.neighbors
            .filter(f => listRemovedNodes.contains(f.value) && !listRemovedNodes.contains(x.value)).size compare y.neighbors
            .filter(f => listRemovedNodes.contains(f.value) && !listRemovedNodes.contains(y.value)).size
        })

        val neighbors = maxCardinalityNode.neighbors

        
        val clique = mutable.HashSet.empty[VertexId]
        clique += maxCardinalityNode.value
        clique ++= maxCardinalityNode.neighbors.map(_.value).filter(f => listRemovedNodes.contains(f))
        
        
        val cliquesAsValues = clique.map(f => mapVertexidToValue.get(f).get)
        setOfCliques += cliquesAsValues
        nodesOfCC ++= cliquesAsValues
        
        listRemovedNodes += maxCardinalityNode.value

          
      }
      //filter out subsets
      return (setOfCliques.filter(f => setOfCliques.forall(p => !f.subsetOf(p) || p == f)), nodesOfCC)
    }

    maxCardinalityList()

    
        
    
    }
  
  //interface between PNKs and NonKEyFinder, returns set of maximal nNonKeys
  def getNonKeys (PNK: mutable.HashSet[mutable.HashSet[Node]], n: Int, nodeSet: mutable.HashSet[Node]) : mutable.HashSet[mutable.HashSet[Node]] = {
    val finalMapDict = mutable.LinkedHashMap.empty[Node, mutable.HashSet[mutable.HashSet[Node]]]
    val listProperties = mutable.ArrayBuffer[Node]()
    val listSubjects = mutable.HashSet.empty[Node]
    val filteredMap = finalMap.filter(f => nodeSet.contains(f._1))
    filteredMap.collect().foreach(f => {
        finalMapDict += f._1 -> f._2
        listProperties += f._1
        listSubjects ++= f._2.flatten
    })

    val initCurInter = listSubjects
    val initCurNKey = mutable.HashSet.empty[Node]
    val initSeenInter = mutable.HashSet.empty[mutable.HashSet[Node]]
    val initNonKeySet = mutable.HashSet.empty[mutable.HashSet[Node]]

    nNonKeyFinder(listProperties(0), initCurInter, initCurNKey, initSeenInter, initNonKeySet, listProperties, finalMapDict, PNK, n, 0)
    
    //filter out subsets
    initNonKeySet.filter(p => initNonKeySet.forall(p2 => !p.subsetOf(p2) || p == p2))
    
  }
    
    

  //checks if currentNKey union pi
  def uncheckedNonKeys (argCurNKey: mutable.HashSet[Node],
      argNonKeySet: mutable.HashSet[mutable.HashSet[Node]],
      argPNK: mutable.HashSet[mutable.HashSet[Node]]) : mutable.HashSet[mutable.HashSet[Node]]  = {
      val result = mutable.HashSet.empty[mutable.HashSet[Node]]
      argPNK.foreach(f => {
        if (argCurNKey.subsetOf(f) && argNonKeySet.forall(p => !f.subsetOf(p))){
          result.+=(f)
        }
      })
      
      return result
    
  }
  
  
  //finds all n-Non-keys but not definitely all maximal
  def nNonKeyFinder (pi : Node,
      curInter : mutable.HashSet[Node],
      curNKey : mutable.HashSet[Node],
      seenInter : mutable.HashSet[mutable.HashSet[Node]],
      nonKeySet : mutable.HashSet[mutable.HashSet[Node]],
      properties : mutable.ArrayBuffer[Node],
      accessfinalMap : mutable.LinkedHashMap[Node, mutable.HashSet[mutable.HashSet[Node]]],
      PNK: mutable.HashSet[mutable.HashSet[Node]],
      n : Int,
      i : Int) : Unit = {

    if(uncheckedNonKeys(curNKey + pi, nonKeySet, PNK).size > 0){
      val piValue = accessfinalMap.get(pi).get
      var selectedExceptionSet = piValue
      piValue.foreach(f => {
          if(curInter.subsetOf(f)){
            var selectedExceptionSet = mutable.HashSet(mutable.HashSet(f))
          } 
        })

        selectedExceptionSet.foreach(s => {
          val newInter = s.intersect(curInter)
          if(newInter.size > 1){
            if(!seenInter.contains(newInter)){
              val nvNKey = curNKey + pi

              if(newInter.size > n){
                nonKeySet += nvNKey
              }
              
              if(i+1 < properties.size){
                nNonKeyFinder(properties(i+1), newInter, nvNKey, seenInter, nonKeySet, properties, accessfinalMap, PNK, n, i+1)
              }
              
            }
          }
          seenInter += newInter
        })

    }
    if(i+1 < properties.size){
      nNonKeyFinder(properties(i+1), curInter, curNKey, seenInter, nonKeySet, properties, accessfinalMap, PNK, n, i+1)
    }
  }

  
  
  
  //HELP FUNCTIONS
  
  //returns complement set
  def getComplement(inputSet :  mutable.HashSet[mutable.HashSet[Node]]) : mutable.HashSet[mutable.HashSet[Node]] = {
    val flatSet = inputSet.flatten
    val complement = mutable.HashSet.empty[mutable.HashSet[Node]]
    inputSet.foreach(f => complement += flatSet.--(f))
    return complement
  }
  
  //returns list of properties from the compSet in descending order
  def getOrderedProperties(compSet :  mutable.HashSet[mutable.HashSet[Node]]) : Seq[Node] = {
    val mapOrderedProperties = mutable.HashMap.empty[Node,Int].withDefaultValue(0)
    compSet.foreach(f => f.foreach(i => mapOrderedProperties(i) += 1))
    val sortedSequence = mapOrderedProperties.toSeq.sortWith(_._2 > _._2)
    sortedSequence.map(_._1)
    
  }
  
  //selects Sets which dont include pi
  def selecSets(pi: Node, compSet:  mutable.HashSet[mutable.HashSet[Node]]) : mutable.HashSet[mutable.HashSet[Node]] = {
    val test = mutable.HashSet.empty[mutable.HashSet[Node]]
    compSet.foreach(f => test += f)
    return test.filter(p => !p.contains(pi))

  }
  
  


  
  //val compSet1 = mutable.HashSet(mutable.HashSet(1,2,4),mutable.HashSet(1,3,4),mutable.HashSet(3,5),mutable.HashSet(4,5))
  
  //returns n-almostKeys for given set of (n+1)-nonKeys
  def keyDerivation(compSet:  mutable.HashSet[mutable.HashSet[Node]]) : mutable.HashSet[mutable.HashSet[Node]] = {

    val keySet = mutable.HashSet.empty[mutable.HashSet[Node]]
    val orderedProperties = getOrderedProperties(compSet)
    var i = 0
    var flag = false
    while(i < orderedProperties.size && !flag){
        val p = orderedProperties(i)
        val selectedCompSets = selecSets(p, compSet) 
        
        if(selectedCompSets.isEmpty){
          keySet += mutable.HashSet(p)
        }
        else{
          //use deepCopy to prevent recursion from changing arguments
          val result = keyDerivation(deepCopyHashSetOfHashSets(selectedCompSets))
          result.foreach(g => {
            val piSet = mutable.HashSet(p)
            piSet ++= g
            keySet += piSet
          })
        }

        compSet.foreach(f => f -= p)
        
        //break if one set is empty
        if(!compSet.forall(p => p.nonEmpty)){
          flag = true
        }
        i += 1
      
      
    }
    return keySet
  }
  
  def deepCopyHashSetOfHashSets(original : mutable.HashSet[mutable.HashSet[Node]]): mutable.HashSet[mutable.HashSet[Node]] = {
    val deepCopy = mutable.HashSet.empty[mutable.HashSet[Node]]
    original.foreach(f => {
      val innerSet = mutable.HashSet.empty[Node]
      f.foreach(g => {
        innerSet += g
      })
      deepCopy += innerSet
    })
    deepCopy
  }


 
    


  
  
  
}
  
  
 