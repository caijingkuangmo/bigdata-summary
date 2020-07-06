package com.laoliu

import org.apache.spark.{SparkConf, SparkContext}

object TestSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Seq((2,"heihei"),(2,"haha"),(None,"yaya"),(4,"fuck")), 2)
    val groupRDD = rdd.groupBy(_._1)
    groupRDD.flatMapValues(v => v +"123").collect().foreach(println)

    sc.stop()
  }

}
