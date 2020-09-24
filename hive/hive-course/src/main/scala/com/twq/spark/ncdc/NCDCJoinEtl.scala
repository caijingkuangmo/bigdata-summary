package com.twq.spark.ncdc

import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

/**
spark-submit --class com.twq.spark.ncdc.NCDCJoinEtl \
--master spark://master:7077 \
--deploy-mode client \
--driver-memory 512m \
--executor-memory 512m \
--total-executor-cores 2 \
--executor-cores 1 \
/home/hadoop-twq/hive-course/hive-course-1.0-SNAPSHOT.jar
  */
object NCDCJoinEtl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NCDCJoinEtl")
      .enableHiveSupport()
      .getOrCreate()

    val ncdcRecords = spark.read.textFile("hdfs://master:9999/user/hadoop-twq/ncdc/rawdata/records")
      .flatMap(line => Option(NcdcRecordParser.fromLine(line)))(Encoders.bean(classOf[NcdcRecordDto]))

    val ncdcStationMetadata = spark.read.textFile("hdfs://master:9999/user/hadoop-twq/ncdc/rawdata/station_metadata")
      .flatMap(line => Option(NcdcStationMetadataParser.fromLine(line)))(Encoders.bean(classOf[StationInfoDto]))

    ncdcRecords.join(ncdcStationMetadata, Seq("stationId"))
      .write
      .partitionBy("year")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ncdc.ncdc_joined_spark")

    spark.stop()
  }
}
