package com.twq.spark.rdd.sources

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/9/2.
  */
object CombineSmallFiles {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test")

    val sc = new SparkContext(conf)

    val wholeTextFiles = sc.wholeTextFiles("hdfs://master:9999/users/hadoop-twq/text/")
    wholeTextFiles.collect()

    wholeTextFiles.coalesce(1).saveAsSequenceFile("hdfs://master:9999/users/hadoop-twq/seq/")
  }
}
