package com.twq.spark.rdd.keyvalue

import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/8/19.
  */
object DogCombineByKeyTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")

    val sc = new SparkContext(conf)


  }

}
