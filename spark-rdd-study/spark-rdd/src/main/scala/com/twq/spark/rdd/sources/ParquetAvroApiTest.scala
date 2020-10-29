package com.twq.spark.rdd.sources

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.databricks.spark.avro._


/**
  * Created by tangweiqun on 2017/8/24.
  *
  */
object ParquetAvroApiTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val personDf =
      spark.sparkContext.parallelize(Seq(Person("jeffy", 30), Person("tomy", 23)), 1).toDF()

    //avro
    personDf.write.mode(SaveMode.Overwrite).avro("hdfs://master:9999/users/hadoop-twq/avro")
    val avroPersonDf = spark.read.avro("hdfs://master:9999/users/hadoop-twq/avro")
    avroPersonDf.show()

    //parquet
    personDf.write.mode(SaveMode.Overwrite).parquet("hdfs://master:9999/users/hadoop-twq/parquet")
    val parquetPersonDF = spark.read.parquet("hdfs://master:9999/users/hadoop-twq/parquet")
    parquetPersonDF.show()

    //json
    personDf.write.mode(SaveMode.Overwrite).json("hdfs://master:9999/users/hadoop-twq/json")
    val jsonPersonDf = spark.read.json("hdfs://master:9999/users/hadoop-twq/json")
    jsonPersonDf.show()
  }

}

