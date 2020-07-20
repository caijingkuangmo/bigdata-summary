package com.twq.dataset.usage

import com.twq.dataset.User
import org.apache.spark.sql.SparkSession
import com.twq.dataset.Utils._

/**
  * Created by tangweiqun on 2017/10/11.
  */
object SortApiTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SortApiTest")
      .getOrCreate()

    import spark.implicits._

    val users = spark.read.json(s"${BASE_PATH}/join/users.json").repartition(2).as[User]
    users.show()
    users.rdd.glom().collect().foreach(users => {
      println("partition...start")
      users.foreach(println(_))
      println("partition...end")
    })

    //1: 按照某个字段或者某种条件对整个Dataset进行排序
    users.sort("age").show()

    users.sort($"age".desc).show()

    //orderBy是基于sort实现的
    users.orderBy("age").show()

    users.orderBy($"age".desc).show()

    //2：使的Dataset在每一个分区内按照某个字段或者条件排序
    val ascSortedDF = users.sortWithinPartitions("age")

    ascSortedDF.rdd.glom().collect().foreach(users => {
      println("partition...start")
      users.foreach(println(_))
      println("partition...end")
    })

    val descSortedDF = users.sortWithinPartitions($"age".desc)

    descSortedDF.rdd.glom().collect().foreach(users => {
      println("partition...start")
      users.foreach(println(_))
      println("partition...end")
    })

    spark.stop()

  }
}
