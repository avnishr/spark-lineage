package com.avnish.sample

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, sum}
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}

object DemoLineage {

  import org.apache.spark.sql._
  import za.co.absa.spline.harvester.SparkLineageInitializer._
/*
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Lineage Demo")
      .master("local[*]")
      .getOrCreate()


    // Initializing library to hook up to Apache Spark
    spark.enableLineageTracking()

    // A business logic of a spark job ...

    var sourceDS = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file:///Users/avnish/src/Lineage/data/input/batch/wikidata.csv")
      .as("source")
    sourceDS = sourceDS
      .filter(sourceDS.col("total_response_size") > 1000)
      .filter(sourceDS.col("count_views") > 10)

    sourceDS = sourceDS.select(sourceDS.col("domain_code").as("domain_code_1"),
                sourceDS.col("count_views").as("count_views_1"), sourceDS.col("total_response_size"))

    val domainMappingDS = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file:///Users/avnish/src/Lineage/data/input/batch/domain.csv")
      .as("mapping")

    var joinedDS = sourceDS
      .join(domainMappingDS, sourceDS.col("domain_code_1") === domainMappingDS.col("d_code"), "left_outer")
        .select(sourceDS.col("count_views_1").as("count"), domainMappingDS.col("d_name").as("domain"),
          domainMappingDS.col("d_code"))

    joinedDS.write.mode(SaveMode.Overwrite).csv("file:///Users/avnish/src/Lineage/data/output/batch/job1_results")
  }*/

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


    var empDS = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\abhi_data_project\\Data_in\\emp_data.csv")
      .as("source")

    val deptDS = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\abhi_data_project\\Data_in\\dept.csv")
      .as("mapping")

    var joinedDS = empDS
      .join(deptDS, empDS.col("dept_id") === deptDS.col("dept_id"), "left_outer").
      select(empDS.col("name"),empDS.col("gender"), deptDS.col("dept_name"),empDS("Salary"))

   // var aggrDF = joinedDS.withColumn("org_name",lit("Accenture"))

    joinedDS.createOrReplaceTempView("TBL")

    var finalDF = joinedDS.groupBy("dept_name").agg(sum("Salary").as("total_sal"))
      //spark.sql("select sum(Salary) as total_sal,dept_name from TBL group by dept_name")
   // finalDF.show()

    finalDF.coalesce(1).write.mode(SaveMode.Overwrite).option("header","true").csv("C:\\abhi_data_project\\Data_out\\output.csv")


  }



}









