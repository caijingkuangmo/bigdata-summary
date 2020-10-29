package com.twq.spark.rdd.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/8/19.
  */
object ActionApiTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")

    val sc = new SparkContext(conf)

    val pairRDD = sc.parallelize[(Int, Int)](Seq((1, 2), (3, 4), (3, 6)), 2)

    pairRDD.collectAsMap()

    pairRDD.lookup(3)
  }

}
