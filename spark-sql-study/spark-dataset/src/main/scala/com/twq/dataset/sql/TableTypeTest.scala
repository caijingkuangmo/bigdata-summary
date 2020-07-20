package com.twq.dataset.sql

import com.twq.dataset.Utils._
import org.apache.spark.sql.SparkSession

/**
  * Created by tangweiqun on 2017/10/11.
  */
object TableTypeTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SqlApiTest")
      .getOrCreate()

    //查看spark sql应用用的是哪一种catalog
    //目前支持hive metastore 和in-memory两种
    //spark-shell默认的值为hive
    //spark-shell --master spark://master:7077 --conf spark.sql.catalogImplementation=in-memory
    spark.conf.get("spark.sql.catalogImplementation")

    spark.sql("CREATE DATABASE IF NOT EXISTS twq4 " +
      "COMMENT 'Test database' LOCATION 'hdfs://master:9999/user/hadoop-twq/spark-db'")

    spark.catalog.setCurrentDatabase("twq4")

    spark.catalog.listTables().show()

    //1：外部表
    spark.catalog.createTable("trackerSession_other", s"${BASE_PATH}/trackerSession")
    spark.sql("select * from trackerSession_other").show()

    //外部表被删除，数据还在
    spark.sql("drop table trackerSession_other")

    spark.catalog.createTable("person_json", s"${BASE_PATH}/people.json", "json")
    spark.sql("select * from person_json").show()

    //2：内部表
    spark.sql("create table person(name string, age int) using parquet")
    val person_other = spark.read.json(s"${BASE_PATH}/people.json")
    person_other.createOrReplaceTempView("person_other")
    spark.sql("insert into table person select name, age from person_other")

    spark.sql("select * from person").show()

    //内部表被删除，数据也被删除了
    spark.sql("drop table person")

    //3：临时视图
    val sessionDf = spark.read.parquet(s"${BASE_PATH}/trackerSession")
    //3.1：session级别的视图
    //第一种创建临时视图的方式
    sessionDf.createTempView("trackerSession")
    sessionDf.createOrReplaceTempView("trackerSession")
    val sessionRecords = spark.sql("select * from trackerSession")
    sessionRecords.show()

    //第二种创建临时视图的方式
    spark.sql("CREATE TEMPORARY VIEW temp_cookie1 AS SELECT * FROM trackerSession WHERE cookie = 'cookie1'")
    val cookie1 = spark.sql("select * from temp_cookie1")
    cookie1.show()

    val personDF = spark.read.json(s"${BASE_PATH}/people.json")
    //3.2：全局级别的视图
    personDF.createOrReplaceGlobalTempView("person_global")
    //需要从spark sql的保留db global_temp 中查询这个全局视图
    spark.sql("select * from global_temp.person_global").show()

    spark.newSession().sql("select * from trackerSession").show()
    spark.newSession().sql("select * from global_temp.person_global").show()

    //catalog table相关元数据操作
    spark.catalog.listTables().show()

    //删除临时视图
    spark.catalog.dropTempView("trackerSession")
    spark.catalog.dropGlobalTempView("person_global")

    spark.stop()
  }
}
