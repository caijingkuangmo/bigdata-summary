package com.twq.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/9/3.
  */
object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test")

    val sc = new SparkContext(conf)

    val lookupTable = Map("plane" -> "sky", "fish" -> "sea", "people" -> "earth")

    val lookupTableB = sc.broadcast(lookupTable)

    val logData = sc.parallelize(Seq("plane", "fish", "duck", "dirty", "people", "plane"), 2)

    logData.foreach(str => {
      val replaceStrOpt = lookupTableB.value.get(str)
      println("element is : " + str)
      if (replaceStrOpt.isDefined) {
        println(s"============找到了[${str}]对应的值[${replaceStrOpt.get}]")
      }
    })
  }
}