package net.sansa_stack.template.spark.rdf
import java.net.URI

import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import java.net.{ URI => JavaURI }
import scala.collection.mutable
import org.apache.spark.graphx.Graph
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model.graph._
import org.apache.jena.riot.Lang

import scala.collection.mutable

object TripleReader {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run()
      case None =>
        println(parser.usage)
    }
  }

  def run(): Unit = {

    val spark = SparkSession.builder
      .appName(s"Triple reader example  ")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("======================================")
    println("|        Triple reader example       |")
    println("======================================")

    /*val triplesRDD = NTripleReader.load(spark, URI.create("src/main/resources/rdf.nt"))

    triplesRDD.take(5).foreach(println(_))

    //triplesRDD.saveAsTextFile(output)
*/
    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)("src/main/resources/data_sets/DB_NaturalPlace.nt")
    triples.cache()
      

  
    val t2 = System.nanoTime()
    val graph = triples.asGraph()
    graph.cache()
    
    graph.edges.take(5).foreach(println)
    println("finish Graph")
    val t3 = System.nanoTime()
    
    val t0 = System.nanoTime()
    val cc = graph.connectedComponents()
    
     
    
    println(cc.triplets.take(5).foreach(println))
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t3 - t2)/1000000000 + "s graph")
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s ConnecteComponents")

    spark.stop

  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Triple reader example") {

    head(" Triple reader example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")
      
    help("help").text("prints this usage text")
  }
}