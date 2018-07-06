package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession

object classDistribution extends App {

  val spark = SparkSession.builder
    .appName(s"WordCount example")
    .master("local[*]")
    .getOrCreate()

  val input = "src/main/resources/page_links_simple.nt"

  val triplesLines = spark.sparkContext.textFile(input)


  triplesLines.cache()

  println(triplesLines.count())
  val removedComments = triplesLines.filter(!_.startsWith("#"))

  val triples = removedComments.map(data => TripleUtils.parsTriples(data))
  
  val subjects = triples.map(s => (s.subject, 1))
  val predicates = triples.map(s => (s.predicate, 1))
  val objects = triples.map(s => (s.`object`, 1))

  val subject_freq = subjects.reduceByKey((a, b) => a + b) //(_+_)
  val predicates_freq = predicates.reduceByKey((a, b) => a + b) //(_+_)
  val objects_freq = objects.reduceByKey((a, b) => a + b) //(_+_)
  
  val sort_subjects = subject_freq.sortBy(_._2, false)

  subject_freq.take(5).foreach(println)
  sort_subjects.take(5).foreach(println)
  predicates_freq.take(5).foreach(println)
  objects_freq.take(5).foreach(println)

  spark.stop()
}

