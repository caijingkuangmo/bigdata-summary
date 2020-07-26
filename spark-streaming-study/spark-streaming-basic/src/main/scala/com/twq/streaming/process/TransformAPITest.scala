package com.twq.streaming.process

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2018/1/6.
  */
object TransformAPITest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    val sc = new SparkContext(sparkConf)

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sc, Seconds(5))

    val lines1 = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER)
    val kvs1 = lines1.map { line =>
      val arr = line.split(" ")
      (arr(0), arr(1))
    }

    val path = "hdfs://master:9999/user/hadoop-twq/spark-course/streaming/keyvalue.txt"
    val keyvalueRDD =
      sc.textFile(path).map { line =>
        val arr = line.split(" ")
        (arr(0), arr(1))
      }

    kvs1.transform { rdd =>
      rdd.join(keyvalueRDD)
    } print()

    //启动Streaming处理流
    ssc.start()

    ssc.stop(false)


    val lines2 = ssc.socketTextStream("master", 9997, StorageLevel.MEMORY_AND_DISK_SER)
    val kvs2 = lines2.map { line =>
      val arr = line.split(" ")
      (arr(0), arr(1))
    }

    kvs1.transformWith(kvs2, (rdd1: RDD[(String, String)], rdd2: RDD[(String, String)]) => rdd1.join(rdd2))

    //等待Streaming程序终止
    ssc.awaitTermination()
  }
}
