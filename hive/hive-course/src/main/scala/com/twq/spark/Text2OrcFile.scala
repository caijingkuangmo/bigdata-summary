package com.twq.spark

import org.apache.spark.sql.SparkSession

object Text2OrcFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("Text2OrcFile")
        .master("local")
        .enableHiveSupport()
        .getOrCreate()

    spark.read.csv("hdfs://master:9999/user/hadoop-twq/hive-course/omneo.csv")
      .toDF("id", "event_id", "event_type", "part_name", "part_number", "version", "payload")
      .write.orc("hdfs://master:9999/user/hadoop-twq/hive-course/orc")

    spark.stop()
  }
}
