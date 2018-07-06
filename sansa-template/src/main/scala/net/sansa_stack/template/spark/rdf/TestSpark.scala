package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession

object TestSpark extends App {

  val spark = SparkSession.builder
    .appName(s"WordCount example")
    .master("local[*]")
    .getOrCreate()

  val input = "src/main/resources/page_links_simple.nt"

  val triplesLines = spark.sparkContext.textFile(input)

  triplesLines.take(5).foreach(println)

  triplesLines.cache()

  println(triplesLines.count())
  val removedComments = triplesLines.filter(!_.startsWith("#"))

  val triples = removedComments.map(data => TripleUtils.parsTriples(data))

  val mapSubject = triples.map(s => (s.subject, 1))

  mapSubject.take(5).foreach(println)

  val subject_freq = mapSubject.reduceByKey((a, b) => a + b) //(_+_)

  subject_freq.take(5).foreach(println)

  spark.stop()
}

object TripleUtils {

  def parsTriples(parsData: String): Triples = {
    val subRAngle = parsData.indexOf('>')
    val predLAngle = parsData.indexOf('<', subRAngle + 1)
    val predRAngle = parsData.indexOf('>', predLAngle + 1)
    var objLAngle = parsData.indexOf('<', predRAngle + 1)
    var objRAngle = parsData.indexOf('>', objLAngle + 1)

    if (objRAngle == -1) {
      objLAngle = parsData.indexOf('\"', objRAngle + 1)
      objRAngle = parsData.indexOf('\"', objLAngle + 1)
    }

    val subject = parsData.substring(1, subRAngle)
    val predicate = parsData.substring(predLAngle + 1, predRAngle)
    val `object` = parsData.substring(objLAngle + 1, objRAngle)

    Triples(subject, predicate, `object`)
  }

}

case class Triples(subject: String, predicate: String, `object`: String) {

  def isLangTag(resource: String) = resource.startsWith("@")
}