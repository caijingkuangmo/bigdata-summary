package com.twq.spark.rdd.sources

import java.sql.{DriverManager, ResultSet}

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat => NewTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat => NewTextOutputFormat}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/8/26.
  */
object FileSystemApiTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test")

    val sc = new SparkContext(conf)

    val data = sc.parallelize(Seq("just test", "hello world"), 1)

    //jdbc mysql and oracle
    def createConnection() = {
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://localhost/test?user=hhh")
    }

    def extractValues(r: ResultSet) = {
      (r.getInt(1), r.getString(2))
    }

    val sql = "select * from test where ? <= id and id <= ?"

    val dataJdbc = new JdbcRDD(sc, createConnection,
      sql, lowerBound = 1, upperBound = 3, numPartitions = 2, mapRow = extractValues)
    dataJdbc.collect()

    data.saveAsTextFile("file:///home/hadoop-twq/spark-course/test")

    //本地文件系统中写读文件
    sc.textFile("file:///home/hadoop-twq/spark-course/echo.sh").collect()

    //hdfs文件系统中读写文件
    // use old api
    data.saveAsTextFile("hdfs://master:9999/users/hadoop-twq/test")
    val keyValueRDD = sc.hadoopFile("hdfs://master:9999/users/hadoop-twq/test/part-00000",
      classOf[TextInputFormat], classOf[LongWritable], classOf[Text]).map {case (key, value) =>
      (key.get(), value.toString)
    }
    keyValueRDD.collect()
    // 当这里说 新老api 都是mapreduce 读写HDFS， spark会把这些api集成进去
    // 新的api 使用抽象类代替了老的接口，这样的优点：在api演化方面更加灵活
    // 新的api 有content的概念，参数可以在这里面，在兼容性方面更好，后面加参数往这里面加

    val data2 = sc.hadoopFile("hdfs://master:9999/users/hadoop-twq/test/part-00000",
      classOf[TextInputFormat], classOf[LongWritable], classOf[Text])  //需要注意的是   新的api读取的数据必须是key-value类型
    // use new api
    data2.saveAsNewAPIHadoopFile[NewTextOutputFormat[LongWritable, Text]](
      "hdfs://master:9999/users/hadoop-twq/test2")
    sc.newAPIHadoopFile("hdfs://master:9999/users/hadoop-twq/test/part-00000",
      classOf[NewTextInputFormat], classOf[LongWritable], classOf[Text]).
      map { case (_, value) => value.toString } collect()

    //s3文件系统中
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "YOUR_KEY_ID")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "YOUR_SECRET")
    data.saveAsTextFile("s3n://bucket/test")
    val s3FileInput = sc.textFile("s3n://bucket/*.log")
    s3FileInput.collect()

  }

}
