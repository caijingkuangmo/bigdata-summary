package com.twq.dataset.datasource.partition

import com.twq.dataset.Utils._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by tangweiqun on 2017/10/11.
  */
object FilePartitionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("FilePartitionTest")
      .getOrCreate()

    val sessions = spark.read.parquet(s"${BASE_PATH}/trackerSession")
    sessions.show()
    sessions.printSchema()

    sessions.createOrReplaceTempView("non_partition_table")
    spark.sql("select * from non_partition_table where day = 20170903").show()  //这个扫描整个表

    //对数据按照年月日进行分区
    // 分区后数据会按照  按年划分目录  名字如year=2020，  年目录又划分月目录， 如mouth=7。。。
    sessions.write.mode(SaveMode.Overwrite).partitionBy("year", "month", "day").parquet(s"${BASE_PATH}/trackerSession_partition")
//    sessions.write.mode(SaveMode.Overwrite).partitionBy("cookie").parquet(s"${BASE_PATH}/trackerSession_partition")

    val partitionDF = spark.read.parquet(s"${BASE_PATH}/trackerSession_partition")
    partitionDF.show()  //这里会把年月日读到列里
    partitionDF.printSchema()

    //用sql查询某20170903这天的数据
    partitionDF.createOrReplaceTempView("partition_table")
    spark.sql("select * from partition_table where day='20200903'").show()  //使用分区字段查询，性能会非常好
//    spark.sql("select * from partition_table where cookie='cookie1'").show()

    //取20170903这天的数据
    val day03DF = spark.read.parquet(s"${BASE_PATH}/trackerSession_partition/year=2017/month=201709/day=20170903")
    day03DF.show()   //这里不会有年月日的列，path上有体现
    day03DF.printSchema()

    //bucket只能用于hive表中
    //而且只用于parquet、json和orc文件格式的文件数据
    sessions.write
      .partitionBy("year")
      .bucketBy(24, "cookie")
      .saveAsTable("session")

    spark.stop()
  }
}
