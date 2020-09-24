package com.twq.douban

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
spark-submit --class com.twq.douban.MovieEtl \
--master spark://master-dev:7077 \
--deploy-mode client \
--driver-memory 512m \
--executor-memory 512m \
--total-executor-cores 2 \
--executor-cores 1 \
--conf spark.douban.movie.path=hdfs://master-dev:9999/user/hadoop-twq/hive-course/douban/movie.csv \
/home/hadoop-twq/hive-course/hive-course-1.0-SNAPSHOT.jar douban movie

  */
object MovieEtl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MovieEtl")
      .enableHiveSupport()
      .getOrCreate()

    val movieDataPath = spark.conf.get("spark.douban.movie.path",
      "hdfs://master-dev:9999/user/hadoop-twq/hive-course/douban/movie.csv")

    val db = args(0)
    val movieTable = args(1)

    import spark.implicits._

    spark.sparkContext
      .textFile(movieDataPath)
      .map(line => MovieDataParser.parseLine(line)).toDS()
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year")
      .saveAsTable(s"${db}.${movieTable}")

    spark.stop()
  }
}
