package com.twq.spark

import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._

object Text2AvroFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("Text2AvroFile")
        .master("local")
        .getOrCreate()

    spark.read.csv("hdfs://master:9999/user/hadoop-twq/hive-course/omneo.csv")
      .toDF("id", "event_id", "event_type", "part_name", "part_number", "version", "payload")
      .write.avro("hdfs://master:9999/user/hadoop-twq/hive-course/avro")

    spark.stop()
  }
}
