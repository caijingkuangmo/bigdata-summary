package com.twq.dataset

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by tangweiqun on 2017/10/11.
  */
object SparkSqlDabblerFirst {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSessionTest")
      .getOrCreate()

    import spark.implicits._

    //创建DataFrame
    val df: DataFrame = spark.read.json("hdfs://master:9999/user/hadoop-twq/spark-course/sql/person.json")

    //用sql的方式访问
    df.createOrReplaceTempView("people")
    val sqlDf: DataFrame = spark.sql("select name from people")
    sqlDf.show()

    //用DataFrame的api
    df.select("name").show()
    df.select($"age" > 23).groupBy($"name").count()

    //用Dataset的api
    val ds: Dataset[Person] = df.as[Person]
    ds.map(p => p.name).show()
    ds.filter(p => p.age > 20)

    spark.stop()

  }
}
