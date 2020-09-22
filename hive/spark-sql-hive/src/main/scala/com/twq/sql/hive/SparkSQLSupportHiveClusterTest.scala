package com.twq.sql.hive

import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by tangweiqun on 2017/10/22.
spark-submit --class com.twq.sql.hive.SparkSQLSupportHiveClusterTest \
--master spark://master:7077 \
--deploy-mode client \
--driver-memory 512m \
--executor-memory 512m \
--total-executor-cores 2 \
--executor-cores 1 \
/home/hadoop-twq/spark-course/spark-sql-hive-1.0-SNAPSHOT.jar
  */
object SparkSQLSupportHiveClusterTest {


  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("SparkSQLSupportHiveClusterTest")
      //.config("spark.sql.warehouse.dir", "hdfs://master:9999/user/hive/warehouse") //如果hive-site.xml已经拷贝到$SPARK_HOME/conf下，则这个可以不配置呢
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    spark.sql("CREATE DATABASE IF NOT EXISTS twq")

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS twq.tracker_session (
        | session_id string,
        | session_server_time string,
        | cookie string,
        | cookie_label string,
        | ip string,
        | landing_url string,
        | pageview_count int,
        | click_count int,
        | domain string,
        | domain_label string)
        |STORED AS PARQUET
      """.stripMargin)

    spark.sql("LOAD DATA INPATH 'hdfs://master:9999/user/hadoop-twq/example/trackerSession' OVERWRITE INTO TABLE twq.tracker_session").show()

    // Queries are expressed in HiveQL
    val trackerSessionDF = spark.sql("SELECT * FROM twq.tracker_session")

    trackerSessionDF.groupBy("cookie").count().show()

    // Aggregation queries are also supported.
    spark.sql("SELECT COUNT(*) FROM twq.tracker_session").show()

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = spark.sql("select cookie, count(*) as cnt from twq.tracker_session group by cookie")

    // The items in DataFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(cookie: String, count: Long) => s"cookie: $cookie, count: $count"
    }
    stringsDS.show()

    spark.stop()
  }

}
