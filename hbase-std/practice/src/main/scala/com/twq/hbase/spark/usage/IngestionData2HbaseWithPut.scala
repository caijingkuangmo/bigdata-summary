package com.twq.hbase.spark.usage

import com.twq.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}

/**
  create 'user',{ NAME => 'segment', BLOOMFILTER => 'NONE', COMPRESSION => 'NONE', DATA_BLOCK_ENCODING => 'FAST_DIFF'}
  */
object IngestionData2HbaseWithPut {
  def main(args: Array[String]): Unit = {
    import com.twq.hbase.spark.HBaseRDDFunctions
    val sparkConf = new SparkConf().setAppName("IngestionData2HbaseWithPut").setMaster("local")

    val sparkContext = new SparkContext(sparkConf)

    val textFile = sparkContext.textFile("hdfs://master:9999/user/hadoop-twq/hbase-course/spark/data.txt")

    val hbaseConf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sparkContext, hbaseConf)

    import com.twq.hbase.spark.HBaseRDDFunctions._

    textFile.hbaseBulkPut(hbaseContext, TableName.valueOf("user"), (line: String) => {
      val tokens = line.split("\\|")
      val put = new Put(Bytes.toBytes(tokens(0)))
      put.addColumn(Bytes.toBytes("segment"), Bytes.toBytes(tokens(1)), Bytes.toBytes(tokens(2)))
      put
    })


    sparkContext.stop()
  }
}
