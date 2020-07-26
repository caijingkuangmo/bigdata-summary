package com.twq.streaming.process

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by tangweiqun on 2018/1/6.
  */
object JoinAPITest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    val sc = new SparkContext(sparkConf)

    // Create the context with a 5 second batch size
    val ssc = new StreamingContext(sc, Seconds(5))

    val lines1 = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER)
    val kvs1 = lines1.map { line =>
      val arr = line.split(" ")
      (arr(0), arr(1))
    }


    val lines2 = ssc.socketTextStream("master", 9997, StorageLevel.MEMORY_AND_DISK_SER)
    val kvs2 = lines2.map { line =>
      val arr = line.split(" ")
      (arr(0), arr(1))
    }

    kvs1.join(kvs2).print()
    kvs1.fullOuterJoin(kvs2).print()
    kvs1.leftOuterJoin(kvs2).print()
    kvs1.rightOuterJoin(kvs2).print()

    //启动Streaming处理流
    ssc.start()

    ssc.stop(false)

    //等待Streaming程序终止
    ssc.awaitTermination()
  }
}
