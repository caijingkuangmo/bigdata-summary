package com.twq

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object FlightAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FlightAnalysis")
      .master("local")
      .getOrCreate()

    // Spark的API读取数据，生成DataFrame
    /*val airportsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/airports.csv")
    airportsDF.printSchema()
    airportsDF.printSchema()
    airportsDF.show(truncate = false, numRows = 100)

    val flightsDF = spark.read.option("inferSchema", "true").csv("data/2008.csv")
    flightsDF.printSchema()
    flightsDF.show()*/

    // case class类型的RDD转成DataFrame
    val rawFlightsRDD = spark.sparkContext.textFile("data/2008.csv")
    val flightsRDD: RDD[Flight] = rawFlightsRDD.flatMap(Flight.parse(_))
    val flightsDF = spark.createDataFrame(flightsRDD)
    //flightsDF.printSchema()
    //flightsDF.show(truncate = false)
    val rawAirportRDD = spark.sparkContext.textFile("data/airports.csv")
    val airportsRDD = rawAirportRDD.flatMap(Airport.parse(_))
    val airportsDF = spark.createDataFrame(airportsRDD)
    //airportsDF.printSchema()
    //airportsDF.show()

    //1、查询出取消的航班
    val canceledFlights = flightsDF.filter(flightsDF("canceled") > 0)
    //canceledFlights.show(truncate = false)

    //2、查出每一个月取消的航班次数
    import spark.implicits._
    val monthCanceledCount = canceledFlights.groupBy($"date.month").count().orderBy($"date.month".asc)
    //monthCanceledCount.show()

    //3、查询出所有两个机场之间的航班数
    spark.conf.set("spark.sql.shuffle.partitions", 4)
    val airportsFlightsCount = flightsDF.select($"origin", $"dest")
      .groupBy($"origin", $"dest").count()
      .orderBy($"count".desc, $"origin", $"dest")
    //airportsFlightsCount.write.mode(SaveMode.Overwrite).csv("data/tmp")

    //4、cube分析数据
    //flightsDF.select($"origin", $"dest").groupBy($"origin", $"dest").count().show(1000)
    //flightsDF.select($"origin", $"dest").cube($"origin", $"dest").count().show(1000)

    flightsDF.cube($"origin", $"dest").agg(Map(
      "*" -> "count",
      "times.actualElapsedTime" -> "avg",
      "distance" -> "avg"
    )).orderBy($"avg(distance)".desc).show()

    // 1.实际飞行时间
    import org.apache.spark.sql.functions._
    flightsDF.agg(
      max("times.actualElapsedTime").alias("max_actualElapsedTime"),
      min("times.actualElapsedTime").alias("min_actualElapsedTime"),
      avg("times.actualElapsedTime").alias("avg_actualElapsedTime"),
      sum("times.actualElapsedTime").alias("total_actualElapsedTime")
    ).show()


    // 2. cute分析机场距离
    import org.apache.spark.sql.functions._
    flightsDF.cube($"origin", $"dest").agg(
      avg("distance").alias("airport_avg_distance"),
      min("times.actualElapsedTime").alias("airport_min_actualElapsedTime"),
      max("times.actualElapsedTime").alias("airport_max_actualElapsedTime"),
      avg("times.actualElapsedTime").alias("airport_avg_actualElapsedTime")
    ).orderBy(col("airport_avg_distance").desc).show()

    //3.航班数
    import org.apache.spark.sql.functions._
    flightsDF.groupBy("origin", "dest")
      .count()
      .join(airportsDF, col("origin") === col("iata"), "left_outer")
      .select(
        col("origin"),
        col("airport").as("origin_airport"),
        col("dest"),
        col("count")
      )
      .join(airportsDF, col("dest") === col("iata"), "left_outer")
      .select(
        col("origin"),
        col("origin_airport"),
        col("dest"),
        col("airport").as("dest_airport"),
        col("count").as("flight_count")
      ).orderBy("origin", "dest").show(false)

    spark.stop()
  }
}
