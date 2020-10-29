package com.twq.spark.rdd

import java.io.File

import org.apache.spark.SparkContext

/**
  * Created by tangweiqun on 2017/9/10.
  */
object LocalSparkTest {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "test")

    val sourceDataRDD = sc.textFile("file:///Users/tangweiqun/test.txt")

    val wordsRDD = sourceDataRDD.flatMap(line => line.split(" "))

    val keyValueWordsRDD = wordsRDD.map(word => (word, 1))

    val wordCountRDD = keyValueWordsRDD.reduceByKey((x, y) => x + y)

    val outputFile = new File("/Users/tangweiqun/wordcount")
    if (outputFile.exists()) {
      val listFile = outputFile.listFiles()
      listFile.foreach(_.delete())
      outputFile.delete()
    }

    wordCountRDD.saveAsTextFile("file:///Users/tangweiqun/wordcount")

    wordCountRDD.collect().foreach(println)

  }
}
