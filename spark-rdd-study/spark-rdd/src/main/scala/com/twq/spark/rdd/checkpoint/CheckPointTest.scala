package com.twq.spark.rdd.checkpoint

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/8/23.
  */
object  CheckPointTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")

    val sc = new SparkContext(conf)

    val pairRDD = sc.parallelize[(Int, Int)](Seq((1, 2), (3, 4), (3, 6)), 2)

    val filterRDD = pairRDD.filter { case (key, value) => key > 2 }

    val mapRDD = filterRDD.map { case (key, value) => (key + 1, value + 1) }

    mapRDD.toDebugString

    mapRDD.localCheckpoint()

    mapRDD.collect()

    mapRDD.toDebugString

    val otherFilterRDD = mapRDD.filter {case (key, value) => key + value > 1}

    val otherMapRDD = otherFilterRDD.map { case (key, value) => (key + 1, value + 1) }

    otherMapRDD.toDebugString

    sc.setCheckpointDir("hdfs://master:9999/users/hadoop-twq/checkpoint")  //hdfs上缓存需要设置 hdfs存储地址

    otherMapRDD.checkpoint()
    otherMapRDD.toDebugString

    val someMapRDD = otherMapRDD.map { case (key, value) => (key + 1, value + 1) }

    someMapRDD.toDebugString
    someMapRDD.collect()

    someMapRDD.checkpoint()//没有用，因为这个rdd已经执行了job了
    someMapRDD.collect()
  }

}
