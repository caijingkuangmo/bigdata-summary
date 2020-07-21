package com.twq.dataset.datasource

import com.twq.dataset.Utils._
import org.apache.spark.sql.SparkSession

/**
  * Created by tangweiqun on 2017/10/11.
  */
object  TableDatasourceTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TableDatasourceTest")
      .master("local")
      .getOrCreate()

    println(spark.conf.get("spark.sql.catalogImplementation"))  //在spark sql中 table默认是有hive管理的
    spark.catalog.listTables().show()

    //1: 将json文件数据保存到spark的table中
    val df = spark.read.json(s"${BASE_PATH}/people.json")
    df.show()

    df.write.saveAsTable("person")
    spark.catalog.listTables().show()

    val tableDF = spark.read.table("person")
    tableDF.show()

    //2: 从临时视图中读取
    df.createOrReplaceTempView("person_2")

    val table2DF = spark.read.table("person_2")
    table2DF.show()

    spark.stop()
  }
}
