package com.twq.spark.rdd.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/8/19.
  */
object CombineByKeyPractice {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")

    val sc = new SparkContext(conf)

    val pairStrRDD = sc.parallelize[(String, Double)](Seq(("coffee", 0.6),
      ("coffee", -0.1), ("panda", -0.3), ("coffee", 0.1)), 2)

    def createCombiner = (label: Double) => new BinaryLabelCounter(0L, 0L) += label

    def mergeValue = (c: BinaryLabelCounter, label: Double) => c += label

    def mergeCombiners = (c1: BinaryLabelCounter, c2: BinaryLabelCounter) => c1 += c2

    // 统计相同key对应的所有值中是正数值的个数以及是负数值的个数
    //需要的三个参数：
    //createCombiner: V => C,
    //mergeValue: (C, V) => C,
    //mergeCombiners: (C, C) => C
    val testCombineByKeyRDD =
      pairStrRDD.combineByKey(createCombiner, mergeValue, mergeCombiners)
    testCombineByKeyRDD.collect()
  }

}

class BinaryLabelCounter(var numPositives: Long = 0L,
                         var numNegatives: Long = 0L) extends Serializable {

  def +=(label: Double): BinaryLabelCounter = {
    if (label > 0) numPositives += 1L else numNegatives += 1L
    this
  }

  def +=(other: BinaryLabelCounter): BinaryLabelCounter = {
    numPositives += other.numPositives
    numNegatives += other.numNegatives
    this
  }

  override def toString: String = s"{numPos: $numPositives, numNeg: $numNegatives}"

}
