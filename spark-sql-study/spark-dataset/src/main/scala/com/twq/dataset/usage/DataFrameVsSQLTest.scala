package com.twq.dataset.usage

import com.twq.dataset.Utils._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by tangweiqun on 2017/10/11.
  */
object DataFrameVsSQLTest extends DFModule {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionApiTest")
      .getOrCreate()

    val personDf = spark.read.json(s"${BASE_PATH}/person.json")

    val temp = personDf.select("name")

    println(temp.schema) //可以加任何的代码

    temp.show()

    val df = cal(personDf) //模块化编程

    df.show()

    val groupByDf = temp.groupBy("name").count()

    groupByDf.show()

    spark.stop()

  }

}
