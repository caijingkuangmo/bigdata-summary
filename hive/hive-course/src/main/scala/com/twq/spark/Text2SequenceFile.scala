package com.twq.spark

import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.SparkSession

object Text2SequenceFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("Text2SequenceFile")
        .master("local")
        .getOrCreate()

    spark.sparkContext.textFile("hdfs://master:9999/user/hadoop-twq/hive-course/omneo.csv")
      .map(line => (NullWritable.get(), line))
      .saveAsSequenceFile("hdfs://master:9999/user/hadoop-twq/hive-course/sequence")

    spark.stop()
  }
}
