package com.twq.spark.rdd.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/8/19.
  */
object CogroupApiTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")

    val sc = new SparkContext(conf)

    val pairRDD = sc.parallelize[(Int, Int)](Seq((1, 2), (3, 4), (3, 6), (5, 6)), 4)

    val otherRDD = sc.parallelize(Seq((3, 9), (4, 5)))

    //res0: Array[(Int, (Iterable[Int], Iterable[Int]))]
    // = Array((4,(CompactBuffer(),CompactBuffer(5))), (1,(CompactBuffer(2),CompactBuffer())),
    // (5,(CompactBuffer(6),CompactBuffer())), (3,(CompactBuffer(6, 4),CompactBuffer(9))))
    pairRDD.cogroup(otherRDD).collect() // 按照键进行汇总，把两个RDD中 key对应的值，分别封装在 两个CompactBuffer对象中

    //groupWith是cogroup的别名，效果和cogroup一摸一样
    pairRDD.groupWith(otherRDD).collect()

    // Array[(Int, (Int, Int))] = Array((3,(4,9)), (3,(6,9)))
    pairRDD.join(otherRDD).collect()  //相当sql中的inner join， 只有key在两个RDD都有的，才会join，主要对两个RDD key对应的值，随机组合呈现

    // Array[(Int, (Int, Option[Int]))]
    // = Array((1,(2,None)), (5,(6,None)), (3,(4,Some(9))), (3,(6,Some(9))))
    pairRDD.leftOuterJoin(otherRDD).collect()  //以左RDD为准所有的展示，右RDD没有的 以None显示

    // Array[(Int, (Option[Int], Int))] = Array((4,(None,5)), (3,(Some(4),9)), (3,(Some(6),9)))
    pairRDD.rightOuterJoin(otherRDD).collect() //以右RDD为准所有的展示，左RDD没有的 以None显示

    // Array[(Int, (Option[Int], Option[Int]))]
    // = Array((4,(None,Some(5))), (1,(Some(2),None)), (5,(Some(6),None)),
    // (3,(Some(4),Some(9))), (3,(Some(6),Some(9))))
    pairRDD.fullOuterJoin(otherRDD).collect()

    // 减掉相同的key, 这个示例减掉了为3的key
    // Array[(Int, Int)] = Array((1,2), (5,6))
    pairRDD.subtractByKey(otherRDD).collect()  //理解为取差集，otherRDD有的key  就不返回
  }

}
