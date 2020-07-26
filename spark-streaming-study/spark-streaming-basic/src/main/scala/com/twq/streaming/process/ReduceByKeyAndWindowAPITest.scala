package com.twq.streaming.process

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2018/1/6.
  */
object ReduceByKeyAndWindowAPITest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    val sc = new SparkContext(sparkConf)

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sc, Seconds(1))

    ssc.checkpoint("hdfs://master:9999/user/hadoop-twq/spark-course/streaming/checkpoint")

    val lines = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER)

    val words = lines.flatMap(_.split(" "))

    //每5秒中，统计前20秒内每个单词出现的次数
    val wordPair = words.map(x => (x, 1))

    val wordCounts =
      wordPair.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(20), Seconds(5))

    wordCounts.print()

    //启动Streaming处理流
    ssc.start()

    ssc.stop(false)






    //接受一个ReduceFunc和一个invReduceFunc
    //滑动时间比较短，窗口长度很长的场景
    val wordCountsOther =
      wordPair.reduceByKeyAndWindow((a: Int, b: Int) => a + b,
        (a: Int, b: Int) => a - b, Seconds(60), Seconds(2))

    wordCountsOther.checkpoint(Seconds(12)) //窗口滑动间隔的5到10倍

    wordCountsOther.print()

    ssc.start()












    //过滤掉value = 0的值
    words.map(x => (x, 1)).reduceByKeyAndWindow((a: Int, b: Int) => a + b,
      (a: Int, b: Int) => a - b,
      Seconds(30), Seconds(10), 4,
      (record: (String, Int)) => record._2 != 0)

    //等待Streaming程序终止
    ssc.awaitTermination()
  }
}
