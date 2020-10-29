package com.twq.spark.rdd.wordcount

import scala.io.Source

/**
  * Created by tangweiqun on 2017/8/22.
  */
object IteratorTest {
  def main(args: Array[String]): Unit = {
    val lines: Iterator[String] = Source.fromFile("test.txt").getLines()

    println(lines.next())

    /*val wordsFlatMap = lines.flatMap(_.split(" "))
    wordsFlatMap.foreach(println(_))

    val wordKeyValue = wordsFlatMap.map(word => (word, 1))
    wordKeyValue.foreach(println(_))*/


  }
}
