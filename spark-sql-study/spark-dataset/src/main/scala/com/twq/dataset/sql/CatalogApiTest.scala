package com.twq.dataset.sql


import com.twq.dataset.Utils._
import org.apache.spark.sql.SparkSession

/**
  * Created by tangweiqun on 2017/10/11.
  */
object CatalogApiTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CatalogApiTest")
      .getOrCreate()

    //查看spark sql应用用的是哪一种catalog
    //目前支持hive metastore 和in-memory两种
    //spark-shell默认的值为hive
    //spark-shell --master spark://master:7077 --conf spark.sql.catalogImplementation=in-memory
    spark.conf.get("spark.sql.catalogImplementation")

    //1：数据库元数据信息
    spark.catalog.listDatabases().show(false)
    spark.catalog.currentDatabase
    val db = spark.catalog.getDatabase("default")
    spark.catalog.databaseExists("twq")

    spark.sql("CREATE DATABASE IF NOT EXISTS twq " +
      "COMMENT 'Test database' LOCATION 'hdfs://master:9999/user/hadoop-twq/spark-db'")

    spark.catalog.setCurrentDatabase("twq")
    spark.catalog.currentDatabase

    //2：表元数据相关信息
    spark.catalog.listTables("twq").show()

    val sessionDf = spark.read.parquet(s"${BASE_PATH}/trackerSession")
    //创建一张表
    sessionDf.createOrReplaceTempView("trackerSession")

    //catalog table相关元数据操作
    spark.catalog.listTables("twq").show()

    //用sql的方式查询表
    val sessionRecords = spark.sql("select * from trackerSession")

    sessionRecords.show()

    spark.catalog.tableExists("log")
    spark.catalog.tableExists("trackerSession")
    spark.catalog.tableExists("twq", "trackerSession") //todo 感觉应该是spark的bug
    spark.catalog.listTables("twq").show()
    spark.catalog.getTable("trackerSession")

    //表的缓存
    spark.catalog.cacheTable("trackerSession")
    spark.catalog.uncacheTable("trackerSession")

    //3：表的列的元数据信息
    spark.catalog.listColumns("trackerSession").show()

    spark.sql("drop table trackerSession")
    spark.sql("drop database twq")
    spark.catalog.setCurrentDatabase("default")
    spark.catalog.listTables().show()

    spark.stop()
  }
}
