package com.twq.spark.ncdc

import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

/**
spark-submit --class com.twq.spark.ncdc.NCDCStationMetaDataEtl \
--master spark://master:7077 \
--deploy-mode client \
--driver-memory 512m \
--executor-memory 512m \
--total-executor-cores 2 \
--executor-cores 1 \
/home/hadoop-twq/hive-course/hive-course-1.0-SNAPSHOT.jar
  */
object NCDCStationMetaDataEtl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NCDCStationMetaDataEtl")
      .enableHiveSupport()
      .getOrCreate()

    spark.read.textFile("hdfs://master:9999/user/hadoop-twq/ncdc/rawdata/station_metadata")
      .flatMap(line => Option(NcdcStationMetadataParser.fromLine(line)))(Encoders.bean(classOf[StationInfoDto]))
      .write.mode(SaveMode.Overwrite).saveAsTable("ncdc.station_metadata")

    spark.stop()
  }
}
