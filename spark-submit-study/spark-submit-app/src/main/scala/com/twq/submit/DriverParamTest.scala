package com.twq.submit

import java.util.concurrent.TimeUnit

import com.twq.spark.rdd.Dog
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by tangweiqun on 2017/8/13.
  * ## driver相关参数
export HADOOP_CONF_DIR=/home/hadoop-twq/hadoop-2.6.5/etc/hadoop
spark-submit --class com.twq.submit.DriverParamTest \
--name "DriverParamTest" \
--master yarn \
--deploy-mode client \
--driver-memory 512m \
--driver-java-options "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -DsleepDuration=30" \
--driver-class-path "spark-rdd-1.0-SNAPSHOT.jar" \
--executor-memory 512m \
--num-executors 2 \
--executor-cores 1 \
--conf spark.wordcount.input.file=hdfs://master:9999/users/hadoop-twq/submitapp/word.txt \
--conf spark.wordcount.output.path=hdfs://master:9999/users/hadoop-twq/submitapp/wordcount \
--conf spark.wordcount.hdfs.master=hdfs://master:9999 \
/home/hadoop-twq/spark-course/spark-submit-app-1.0-SNAPSHOT.jar \
2

  -- 使用--conf spark.driver.extraJavaOptions传递jvm的系统参数
export HADOOP_CONF_DIR=/home/hadoop-twq/hadoop-2.6.5/etc/hadoop
spark-submit --class com.twq.submit.DriverParamTest \
--name "DriverParamTest" \
--master yarn \
--deploy-mode client \
--driver-memory 512m \
--conf "spark.driver.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -DsleepDuration=3" \
--driver-class-path "spark-rdd-1.0-SNAPSHOT.jar" \
--executor-memory 512m \
--num-executors 2 \
--executor-cores 1 \
--conf spark.wordcount.input.file=hdfs://master:9999/users/hadoop-twq/submitapp/word.txt \
--conf spark.wordcount.output.path=hdfs://master:9999/users/hadoop-twq/submitapp/wordcount \
--conf spark.wordcount.hdfs.master=hdfs://master:9999 \
/home/hadoop-twq/spark-course/spark-submit-app-1.0-SNAPSHOT.jar \
2
  */
object DriverParamTest {
  private val logger = LoggerFactory.getLogger("WordCount")

  def main(args: Array[String]): Unit = {

    if (args.size != 1) {
      logger.error("arg for partition number is empty")
      System.exit(-1)
    }

    val numPartitions = args(0).toInt

    val conf = new SparkConf()

    //conf.setAppName("word count")

    // custom spark conf test
    // 最好只在driver端获取，不建议在Executor获取该值
    val inputFile = conf.getOption("spark.wordcount.input.file")
      .getOrElse(sys.error("spark.wordcount.input.file must set"))
    logger.info(s"inputFile ========== ${inputFile}")

    val outputPath = conf.getOption("spark.wordcount.output.path")
      .getOrElse(sys.error("spark.wordcount.output.path must set"))
    logger.info(s"outputPath ========== ${outputPath}")

    val hdfsMater = conf.getOption("spark.wordcount.hdfs.master")
      .getOrElse(sys.error("spark.wordcount.input.path must set"))
    logger.info(s"hdfsMater ========== ${hdfsMater}")

    // test driver options中的属性
    val sleepDuration = System.getProperty("sleepDuration", "20").toInt
    logger.info(s"sleepDuration ========== ${sleepDuration}")

    val sc = new SparkContext(conf)

    val inputRdd: RDD[(LongWritable, Text)] = sc.hadoopFile(inputFile,
      classOf[TextInputFormat], classOf[LongWritable], classOf[Text])

    val words: RDD[String] = inputRdd.flatMap(_._2.toString.split(" "))

    val wordCount: RDD[(String, Int)] = words.map(word => (word, 1))

    val counts: RDD[(String, Int)] =
      wordCount.reduceByKey(new HashPartitioner(numPartitions), (x, y) => x + y)

    val path = new Path(outputPath)

    val configuration = new Configuration()
    configuration.set("fs.defaultFS", hdfsMater)

    val fs = path.getFileSystem(configuration)

    if (fs.exists(path)) {
      fs.delete(path, true)
    }

    counts.saveAsTextFile(path.toString)

    // test driver class path , 依赖spark-rdd-1.0-SNAPSHOT.jar包
    val dog1 = new Dog()
    dog1.setName("one")

    val dog2 = new Dog()
    dog2.setName("two")

    val dog3 = new Dog()
    dog3.setName("three")

    //睡sleepDuration秒，为了我们查看进程
    TimeUnit.SECONDS.sleep(sleepDuration)
    sc.stop()
  }

}
