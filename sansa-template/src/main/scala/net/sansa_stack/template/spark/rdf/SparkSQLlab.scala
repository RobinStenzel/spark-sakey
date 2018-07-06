package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession

object  SparkSQLlab  extends App{



val  input  =  "src/main/resources/rdf.nt"  // args(0)  
  
val  spark  =SparkSession.builder  
            .master("local[*]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  
            .appName("SparkSQL example")  
            .getOrCreate()  
  
import  spark.implicits._  
 
val  tripleDF  = spark.sparkContext.textFile(input)
                .map(TripleUtils.parsTriples)  
                .toDF()  
 
tripleDF.show()  
  
//tripleDF.collect().foreach(println(_))  

tripleDF.createOrReplaceTempView("triple")  

val  sqlText  =  "SELECT * from triple where subject = 'http://commons.dbpedia.org/resource/Category:Events'" 

val  triplerelatedtoEvents  = spark.sql(sqlText)  
  
//triplerelatedtoEvents.collect().foreach(println(_))
val  subjectdistribution  = spark.sql("select subject, count(*) from triple group by subject") 
//
//println("subjectdistribution:")  
subjectdistribution.collect()

}