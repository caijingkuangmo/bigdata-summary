package com.twq.spark.rdd.partition

import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/8/19.
  */
object PartitionerTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")

    val sc = new SparkContext(conf)

    val pairRDD = sc.parallelize[(String, Int)](Seq((("hello",1)), ("world", 1),
      ("word", 1), ("count", 1), ("count", 1), ("word", 1), ("as", 1),
      ("example", 1), ("hello", 1), ("word", 1), ("count", 1),
      ("hello", 1), ("word", 1), ("count", 1)), 5)

    val hashPartitionedRDD = pairRDD.partitionBy(new HashPartitioner(2))
    hashPartitionedRDD.glom().collect()

    val partitionedRDD = pairRDD.partitionBy(new RangePartitioner[String, Int](2, pairRDD))
    partitionedRDD.glom().collect()

    val partitionedDescRDD = pairRDD.partitionBy(new RangePartitioner[String, Int](2, pairRDD, false))
    partitionedDescRDD.glom().collect()

    val hdfsFileRDD = sc.textFile("hdfs://master:9999/users/hadoop-twq/word.txt", 1000)
    hdfsFileRDD.partitions.size // 1000
    //我们通过coalesce来降低分区数量的目的是：
    //分区太多，每个分区的数据量太少，导致太多的task，我们想减少task的数量，所以需要降低分区数
    //第一个参数表示我们期望的分区数
    //第二个参数表示是否需要经过shuffle来达到我们的分区数
    val coalesceRDD = hdfsFileRDD.coalesce(100, false)
    coalesceRDD.partitions.size //100

    //从1000个分区一下子降到2个分区
    //这里会导致1000个map计算只在2个分区上执行，会导致性能问题
    hdfsFileRDD.map(_ + "test").coalesce(2, true)


  }

}
