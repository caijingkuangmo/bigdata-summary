package com.twq.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/8/24.
  */
object RDDCreationApiTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")

    val sc = new SparkContext(conf)


    sc.getConf.remove("spark.default.parallelism")

    //创建RDD的方法:
    //1: 从一个稳定的存储系统中，比如hdfs文件,或者本地文件系统
    val hdfsFileRDD = sc.textFile("hdfs://master:9999/users/hadoop-twq/word.txt")
    hdfsFileRDD.count()

    //2: 从一个已经存在的RDD中, 即RDD的transformation api
    //以下是一个RDD的transformation
    val mapRDD = hdfsFileRDD.map(x => x + "test")
    mapRDD.count()

    //3: 从一个已经存在于内存中的列表,  可以指定分区，如果不指定的话分区数为所有executor的cores数
    val listRDD = sc.parallelize[Int](Seq(1, 2, 3, 3, 4), 2)
    listRDD.collect()
    listRDD.glom().collect()

    val rangeRDD = sc.range(0, 10, 2, 4)
    rangeRDD.collect()

    val makeRDD = sc.makeRDD(Seq(1, 2, 3, 3))
    makeRDD.collect()

    val makeRDDWithLocations =
      sc.makeRDD(Seq((Seq(1, 2), Seq("172.26.232.93")), (Seq(3, 3, 4), Seq("172.26.232.93"))))
    makeRDDWithLocations.collect()
    makeRDD.glom().collect()

    val defaultPartitionRDD = sc.parallelize[Int](Seq(1, 2, 3, 3, 4))
    defaultPartitionRDD.partitions

    //spark-shell --master spark://master:7077 --conf spark.default.parallelism=3
    sc.getConf.set("spark.default.parallelism", "3")

    // 说明map和flatMap的区别
    val testMapRDD = makeRDD.map(x => x + 1)
    testMapRDD.collect()
    val flatMapRDD = makeRDD.flatMap(x => 0 to x)
    flatMapRDD.collect()
  }
}
