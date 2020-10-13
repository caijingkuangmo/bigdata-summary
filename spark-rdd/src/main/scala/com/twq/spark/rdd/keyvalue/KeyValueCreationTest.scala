package com.twq.spark.rdd.keyvalue

import com.twq.spark.rdd.User
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/8/19.
  */
object KeyValueCreationTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")

    val sc = new SparkContext(conf)

    val kvPairRDD =
      sc.parallelize(Seq(("key1", "value1"), ("key2", "value2"), ("key3", "value3")))
    kvPairRDD.collect()

    val personSeqRDD =
      sc.parallelize(Seq(User("jeffy", 30), User("kkk", 20), User("jeffy", 30), User("kkk", 30)))
    //将RDD变成二元组类型的RDD
    val keyByRDD = personSeqRDD.keyBy(x => x.userId)  //keyBy  函数的返回值作为key
    keyByRDD.collect()

    val keyRDD2 = personSeqRDD.map(user => (user.userId, user))  //类似于上面keyBy的效果

    val groupByRDD = personSeqRDD.groupBy(user => user.userId) // 按照函数返回值进行分组，这个值相同的元素放入同一组中，这里就是名字相同的，user放一起
    groupByRDD.glom().collect()

    val rdd1 = sc.parallelize(Seq("test", "hell"))
    rdd1.map(str => (str, 1))
    rdd1.collect()

  }

}
