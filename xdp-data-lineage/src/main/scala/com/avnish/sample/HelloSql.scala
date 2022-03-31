package com.avnish.sample

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}

object SQLLineage {

  import org.apache.spark.sql._
  import za.co.absa.spline.harvester.SparkLineageInitializer._

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Lineage Demo")
      .master("local[*]")
      .getOrCreate()


    // Initializing library to hook up to Apache Spark
    spark.enableLineageTracking()

    // A business logic of a spark job ...

    var ds1 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file:///Users/avnish/src/Lineage/data/input/batch/wikidata.csv")
      .as("source")

//    ds1.createOrReplaceTempView("wikidata")
//
//    val ds2 = spark.read
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .csv("file:///Users/avnish/src/Lineage/data/input/batch/domain.csv")
//      .as("mapping")
//
//    ds2.createOrReplaceTempView("domain")
//
//    val finalDf = spark.sql("Select count_views as count, d_code, d_name as domain From " +
//      "wikidata w left join domain d on w.domain_code = d.d_code")


    ds1.write.mode(SaveMode.Overwrite).csv("file:///Users/avnish/src/Lineage/data/output/batch/job1_results")
  }
}









