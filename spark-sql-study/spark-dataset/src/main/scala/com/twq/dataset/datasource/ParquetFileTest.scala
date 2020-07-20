package com.twq.dataset.datasource

import com.twq.dataset.Utils._
import org.apache.spark.sql.SparkSession

/**
  * Created by tangweiqun on 2017/10/11.
  */
object ParquetFileTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ParquetFileTest")
      .getOrCreate()

    //1: 将json文件数据转化成parquet文件数据
    val df = spark.read.json(s"${BASE_PATH}/people.json")
    df.show()

    //gzip、lzo、snappy
    df.write.option("compression", "snappy").parquet(s"${BASE_PATH}/parquet")
    //2: 读取parquet文件
    val parquetDF = spark.read.parquet(s"${BASE_PATH}/parquet")
    parquetDF.show()

    //3: parquet schema merge
    //全局设置spark.sql.parquet.mergeSchema = true
    df.toDF("age", "first_name").write.parquet(s"${BASE_PATH}/parquet_schema_change")
    val changedDF = spark.read.parquet(s"${BASE_PATH}/parquet_schema_change")
    changedDF.show()

    val schemaMergeDF = spark.read.option("mergeSchema", "true").parquet(s"${BASE_PATH}/parquet",
      s"${BASE_PATH}/parquet_schema_change")
    schemaMergeDF.show()

    spark.stop()
  }
}
