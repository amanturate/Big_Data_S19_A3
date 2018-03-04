package Assignment_19

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}


object Assignment19_3 extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  val spark = SparkSession.builder()
    .master("local")
    .appName("example")
    .config("spark.sql.warehouse.dir", "file:///C:")
    .getOrCreate()
  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext
  import sqlContext.implicits._
  val myList = 1 to 100 toList
  val mydf = myList.toDF()
  println("Reading contents of a data frame: ")
  mydf.show(100)

  mydf.write.parquet("C:/ACADGILD/Big Data/SESSION_19/ASGN.parquet")
  sqlContext.read.parquet("C:/ACADGILD/Big Data/SESSION_19/ASGN.parquet").show(100)


}
