package com.twq.spark.rdd.sources

import java.io.FileOutputStream

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/9/2.
  */
object BinaryDataFileFormat {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test")

    val sc = new SparkContext(conf)

    val wholeTextFiles = sc.wholeTextFiles("hdfs://master:9999/users/hadoop-twq/text/")
    wholeTextFiles.collect()
    // 和wholeTextFiles效果类似，都是把文件名当key，而文件内容当value
    // wholeTextFiles是读取出来的是文件的文本内容，而binaryFiles读取出来的是文件二进制数据流(如果要查看具体内容，可以将二进制流转化成String)
    val binaryFilesRDD = sc.binaryFiles("hdfs://master:9999/users/hadoop-twq/text/")

    binaryFilesRDD.collect()

    binaryFilesRDD.map { case (fileName, stream) =>
      (fileName, new String(stream.toArray()))
    }.collect()


    //可以用于将hdfs上的脚本同步到每一个executor上
    val binaryFilesStreams = binaryFilesRDD.collect()
    val binaryFilesStreamsB = sc.broadcast(binaryFilesStreams)

    val data = sc.parallelize(Seq(2, 3, 5, 2, 1), 2)
    data.foreachPartition(iter => {
      val allFileStreams = binaryFilesStreamsB.value
      allFileStreams.foreach { case (fileName, stream) =>
        val inputStream = stream.open()
        val fileOutputStream = new FileOutputStream(s"/local/path/fileName-${fileName}")

        val buf = new Array[Byte](4096)
        var hasData = true

        while (true) {
          val r = inputStream.read(buf)
          if (r == -1) hasData = false
          fileOutputStream.write(buf, 0, r)
        }
      }
    })


    val binaryFileData = sc.parallelize[Array[Byte]](List(Array[Byte](2, 3),
      Array[Byte](3, 4), Array[Byte](5, 6)))

    binaryFileData.saveAsTextFile("hdfs://master:9999/users/hadoop-twq/binary/")

    val binaryRecordsRDD = sc.binaryRecords("hdfs://master:9999/users/hadoop-twq/binary/part-00002", 2)
    binaryRecordsRDD.collect()
  }
}
