package com.twq.spark.rdd

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/8/24.
  */
object BaseActionApiTest {

  def getInitNumber(source: String): Int = {
    println(s"get init number from ${source}, may be take much time........")
    TimeUnit.SECONDS.sleep(2)
    1
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test")

    val sc = new SparkContext(conf)

    val listRDD = sc.parallelize[Int](Seq(1, 2, 4, 3, 3, 6), 2)

    listRDD.collect() //收集rdd的所有数据

    listRDD.take(2) // 取前两个元素

    listRDD.top(2)  // 取最大的两个

    listRDD.first()  // 取第一个

    listRDD.min()  //取最小

    listRDD.max()(new MyOrderingNew)  //排序后取第一个元素，默认是从大到小，所以取最大

    listRDD.takeOrdered(2)(new MyOrderingNew)  //排序后  取前两个

    listRDD.foreach(x => {
      val initNumber = getInitNumber("foreach")
      println(x + initNumber + "==================")
    })

    listRDD.foreachPartition(iterator => {
      //和foreach api的功能是一样，只不过一个是将函数应用到每一条记录，这个是将函数应用到每一个partition
      //如果有一个比较耗时的操作，只需要每一分区执行一次这个操作就行，则用这个函数
      //这个耗时的操作可以是连接数据库等操作，不需要计算每一条时候去连接数据库，一个分区只需连接一次就行
      val initNumber = getInitNumber("foreachPartition")
      iterator.foreach(x => println(x + initNumber + "================="))
    })

    listRDD.reduce((x, y) => x + y)

    listRDD.treeReduce((x, y) => x + y)

    //和reduce的功能类似，只不过是在计算每一个分区的时候需要加上初始值1，最后再将每一个分区计算出来的值相加再加上这个初始值
    listRDD.fold(0)((x, y) => x + y)

    //先初始化一个我们想要的返回的数据类型的初始值
    //然后在每一个分区对每一个元素应用函数一(acc, value) => (acc._1 + value, acc._2 + 1)进行聚合
    //最后将每一个分区生成的数据应用函数(acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)进行聚合
    listRDD.aggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    listRDD.treeAggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

  }

}

class MyOrderingNew extends Ordering[Int] {
  override def compare(x: Int, y: Int): Int = {
    y - x
  }
}
