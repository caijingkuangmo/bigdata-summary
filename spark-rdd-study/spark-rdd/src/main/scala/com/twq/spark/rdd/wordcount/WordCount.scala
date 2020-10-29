package com.twq.spark.rdd.wordcount

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/8/22.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")

    val sc = new SparkContext(conf)

    val inputRdd: RDD[(LongWritable, Text)] =
      sc.hadoopFile("hdfs://master:9999/users/hadoop-twq/word.txt",
        classOf[TextInputFormat], classOf[LongWritable], classOf[Text])

    val words: RDD[String] = inputRdd.flatMap(_._2.toString.split(" "))

    val wordCount: RDD[(String, Int)] = words.map(word => (word, 1))

    val counts: RDD[(String, Int)] = wordCount.reduceByKey(new HashPartitioner(2), (x, y) => x + y)

    counts.saveAsTextFile("hdfs://master:9999/users/hadoop-twq/wordcount")

    val testRDD = sc.parallelize(Seq(1, 2, 4, 5, 6))

    sc.stop()
  }

}
