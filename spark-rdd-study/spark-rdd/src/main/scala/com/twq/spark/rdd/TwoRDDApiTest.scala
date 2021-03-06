package com.twq.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/8/19.
  */
object TwoRDDApiTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")

    val sc = new SparkContext(conf)

    val oneRDD = sc.parallelize[Int](Seq(1, 2, 3), 3)

    val otherRDD = sc.parallelize(Seq(3, 4, 5), 3)

    val unionRDD = oneRDD.union(otherRDD)
    unionRDD.collect() // Array[Int] = Array(1, 2, 3, 3, 4, 5)

    val plusPlusRDD = oneRDD ++ otherRDD
    plusPlusRDD.collect() // Array[Int] = Array(1, 2, 3, 3, 4, 5)

    val thirdRDD = sc.parallelize(Seq(5, 5, 5), 3)
    val unionAllRDD = sc.union(Seq(oneRDD, otherRDD, thirdRDD))
    oneRDD.union(otherRDD).union(thirdRDD).collect()
    unionAllRDD.collect()

    val intersectionRDD = oneRDD.intersection(otherRDD)
    intersectionRDD.collect() // Array[Int] = Array(3)

    val subtractRDD = oneRDD.subtract(otherRDD)
    subtractRDD.collect() // Array[Int] = Array(1, 2)

    // Array[(Int, Int)] = Array((1,3), (1,4), (1,5), (2,3), (2,4), (2,5), (3,3), (3,4), (3,5))
    val cartesianRDD = oneRDD.cartesian(otherRDD)
    cartesianRDD.collect()

    //要求两个RDD有相同的元素个数, 分区也得是一样的
    val zipRDD = oneRDD.zip(otherRDD)
    zipRDD.collect() // Array[(Int, Int)] = Array((1,3), (2,4), (3,5))

    //要求两个rdd需要有相同的分区数，但是每一个分区可以不需要有相同的元素个数
    val zipPartitionRDD =
      oneRDD.zipPartitions(otherRDD)((iterator1, iterator2)
      => Iterator(iterator1.sum + iterator2.sum))
    zipPartitionRDD.collect() // Array[Int] = Array(0, 4, 6, 8)


    val zipPartition3RDD =
      oneRDD.zipPartitions(otherRDD, thirdRDD)((iterator1, iterator2, iterator3)
      => Iterator(iterator1.sum + iterator2.sum + iterator3.sum))
    zipPartition3RDD.collect()

  }

}
