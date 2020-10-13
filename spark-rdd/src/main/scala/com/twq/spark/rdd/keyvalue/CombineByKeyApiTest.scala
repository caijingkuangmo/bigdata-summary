package com.twq.spark.rdd.keyvalue

import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by tangweiqun on 2017/8/19.
  */
object CombineByKeyApiTest {

  def test1[C: ClassTag]() = {
    println(reflect.classTag[C].runtimeClass.getName)
  }

  def test2[C]()(implicit ct: ClassTag[C]) = {
    println(Option(reflect.classTag[C]).map(_.runtimeClass.getName))
  }


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CombineByKeyApiTest")

    val sc = new SparkContext(conf)

    val pairStrRDD = sc.parallelize[(String, Int)](Seq(("coffee", 1),
      ("coffee", 2), ("panda", 3), ("coffee", 9)), 2)

    def createCombiner = (value: Int) => (value, 1)

    def mergeValue = (acc: (Int, Int), value: Int) => (acc._1 + value, acc._2 + 1)

    def mergeCombiners = (acc1: (Int, Int), acc2: (Int, Int)) =>
      (acc1._1 + acc2._1, acc1._2 + acc2._2)

    //功能：对pairStrRDD这个RDD统计每一个相同key对应的所有value值的累加值以及这个key出现的次数
    //需要的三个参数：
    //createCombiner: V => C,  ==> Int -> (Int, Int)
    //mergeValue: (C, V) => C,    ==> ((Int, Int), Int) -> (Int, Int)
    //mergeCombiners: (C, C) => C    ==> ((Int, Int), (Int, Int)) -> (Int, Int)
    val testCombineByKeyRDD =
      pairStrRDD.combineByKey(createCombiner, mergeValue, mergeCombiners)
    testCombineByKeyRDD.collect()


    val pairRDD = sc.parallelize[(Int, Int)](Seq((1, 2), (3, 4), (3, 6), (5, 6)), 2)
      //.partitionBy(new HashPartitioner(2))

    val combineByKeyRDD = pairRDD.combineByKeyWithClassTag[(Int, Int)](
      createCombiner, mergeValue, mergeCombiners,
      new HashPartitioner(2), true, new JavaSerializer(sc.getConf))
    combineByKeyRDD.collect()


    // 指定初始值 累加 统计次数，多元操作
    pairRDD.aggregateByKey((0, 0))( // createCombiner = mergeValue((0, 0), v)
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), //mergeValue
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) // mergeCombiners
    ).collect()
    // 效果和下面的是一样的
    def createCombinerAggregate = (value: Int) => mergeValueAggregate((0, 0), value)
    def mergeValueAggregate = (acc: (Int, Int), v: Int) => (acc._1 + v, acc._2 + 1)
    def mergeCombinersAggregate = (acc1: (Int, Int), acc2: (Int, Int)) =>
      (acc1._1 + acc2._1, acc1._2 + acc2._2)
    pairRDD.combineByKey(createCombinerAggregate,
      mergeValueAggregate, mergeCombinersAggregate).collect()


    // createCombiner = (v: V) => v
    // mergeValue = (x, y) => x + y
    // mergeCombiners = (x, y) => x + y
    // 基于key  对值累加
    pairRDD.reduceByKey((x, y) => x + y).collect()
    //效果和下面的是一样的
    def createCombinerReduce = (value: Int) => value
    def mergeValueReduce = (v: Int, value: Int) => v + value
    def mergeCombinersReduce = (v: Int, value: Int) => v + value
    pairRDD.combineByKey(createCombinerReduce, mergeValueReduce, mergeCombinersReduce).collect()

    // createCombiner = (v: V) => mergeValue(0, v)
    // mergeValue = (x, y) => x + y
    // mergeCombiners = (x, y) => x + y
    // 和reduceByKey效果差不多，只是给定了累加的起始值
    pairRDD.foldByKey(0)((x, y) => x + y).collect()
    //效果和下面的是一样的
    def createCombinerFold = (value: Int) => mergeValueFold(0, value)
    def mergeValueFold = (v: Int, value: Int) => v + value
    def mergeCombinersFold = (v: Int, value: Int) => v + value
    pairRDD.combineByKey(createCombinerFold, mergeValueFold, mergeCombinersFold).collect()

    //createCombiner = (v: V) => CompactBuffer(v)
    //mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
    //mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
    // 基于key相同的  分组
    pairRDD.groupByKey().collect()
    //效果和下面的是一样的
    def createCombinerGroup = (value: Int) => ArrayBuffer(value)
    def mergeValueGroup = (buf: ArrayBuffer[Int], value: Int) => buf += value
    def mergeCombinersGroup = (buf1: ArrayBuffer[Int], buf2: ArrayBuffer[Int]) => buf1 ++= buf2
    pairRDD.combineByKey(createCombinerGroup, mergeValueGroup, mergeCombinersGroup,
      new HashPartitioner(2), false).collect()


    val rdd = sc.parallelize(Seq(1,2,2,3,1))
    val distinctRDD = rdd.distinct()  //对rdd数据去重
    distinctRDD.collect()

    pairRDD.reduceByKeyLocally((x, y) => x + y)  //效果和reduceByKey差不多，只是返回值类型 变成了map类型

  }

}
