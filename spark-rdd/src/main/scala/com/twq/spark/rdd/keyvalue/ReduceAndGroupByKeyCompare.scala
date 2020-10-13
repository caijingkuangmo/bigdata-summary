package com.twq.spark.rdd.keyvalue

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


/**
  * Created by tangweiqun on 2017/8/19.
  */
object ReduceAndGroupByKeyCompare {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")

    val sc = new SparkContext(conf)

    val pairRDD = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 1), ("a", 2),
        ("c", 4), ("b", 1), ("a", 1), ("a", 1)), 3)
    //结果都是 res1: Array[(String, Int)] = Array((b,3), (a,5), (c,5))
    pairRDD.reduceByKey(new HashPartitioner(2), _ + _).collect() //先map端汇总，再shuffle重分区，reduce端再汇总

    pairRDD.groupByKey(new HashPartitioner(2)).map(t => (t._1, t._2.sum)).collect()  //直接shuffle重分区，再reduce端汇总

    //需要对同一个key下的所有value值进行排序
    pairRDD.groupByKey().map { case (key, iter) =>
      val sortedValues = iter.toArray.sorted
      (key, sortedValues)
    }.collect()

    //对于一个key对应的value有很多数据的话，groupByKey可能发生OOM，可以通过重新设计key来消除这个OOM
  }

}
