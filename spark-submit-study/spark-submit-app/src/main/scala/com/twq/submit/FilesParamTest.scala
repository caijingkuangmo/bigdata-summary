package com.twq.submit

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.twq.spark.rdd.{Dog, TrackerLog, TrackerSession}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by tangweiqun on 2017/8/13.
  * -- --files and --properties-file
spark-submit --class com.twq.submit.FilesParamTest \
--name "FilesParamTest" \
--master yarn \
--deploy-mode client \
--driver-memory 512m \
--jars "spark-rdd-1.0-SNAPSHOT.jar" \
--packages org.apache.parquet:parquet-avro:1.8.1 \
--executor-memory 512m \
--num-executors 2 \
--executor-cores 1 \
--files /home/hadoop-twq/spark-course/wordcount.properties \
--properties-file /home/hadoop-twq/spark-course/spark-wordcount.conf \
/home/hadoop-twq/spark-course/spark-submit-app-1.0-SNAPSHOT.jar \
2

  --queue
  export HADOOP_CONF_DIR=/home/hadoop-twq/hadoop-2.6.5/etc/hadoop
spark-submit --class com.twq.submit.FilesParamTest \
--name "FilesParamTest" \
--master yarn \
--deploy-mode client \
--driver-memory 512m \
--jars "spark-rdd-1.0-SNAPSHOT.jar" \
--packages org.apache.parquet:parquet-avro:1.8.1 \
--executor-memory 512m \
--num-executors 2 \
--executor-cores 1 \
--files wordcount.properties \
--properties-file spark-wordcount.conf \
--queue hadoop-twq \
/home/hadoop-twq/spark-course/spark-submit-app-1.0-SNAPSHOT.jar \
2

    --archives
spark-submit --class com.twq.submit.FilesParamTest \
--name "FilesParamTest" \
--master yarn \
--deploy-mode client \
--driver-memory 512m \
--archives "/home/hadoop-twq/spark-course/submitapp.har/part-0" \
--packages org.apache.parquet:parquet-avro:1.8.1 \
--executor-memory 512m \
--num-executors 2 \
--executor-cores 1 \
--files wordcount.properties \
--properties-file spark-wordcount.conf \
--queue hadoop-twq \
/home/hadoop-twq/spark-course/spark-submit-app-1.0-SNAPSHOT.jar \
2
  */
object FilesParamTest {
  private val logger = LoggerFactory.getLogger("WordCount")

  def main(args: Array[String]): Unit = {

    if (args.size != 1) {
      logger.error("arg for partition number is empty")
      System.exit(-1)
    }

    val numPartitions = args(0).toInt

    val conf = new SparkConf()

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "com.twq.submit.kryo.CustomKryoRegistrator")

    //conf.setAppName("word count")

    // custom spark conf test
    // 最好只在driver端获取，不建议在Executor获取该值
    val inputFile = conf.getOption("spark.wordcount.input.file")
      .getOrElse(sys.error("spark.wordcount.input.file must set"))
    logger.info(s"inputFile ========== ${inputFile}")

    val outputPath = conf.getOption("spark.wordcount.output.path")
      .getOrElse(sys.error("spark.wordcount.output.path must set"))
    logger.info(s"outputPath ========== ${outputPath}")

    val dogOutputPath = conf.getOption("spark.wordcount.dog.output.path")
      .getOrElse(sys.error("spark.wordcount.dog.output.path must set"))
    logger.info(s"dogOutputPath ========== ${dogOutputPath}")

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

    val path = deleteExistFile(outputPath, hdfsMater)

    counts.saveAsTextFile(path)

    countDogs

    buildDogs(dogOutputPath, hdfsMater, counts)

    //睡sleepDuration秒，为了我们查看进程
    TimeUnit.SECONDS.sleep(sleepDuration)
    sc.stop()
  }

  private def countDogs = {
    // test driver class path , 依赖spark-rdd-1.0-SNAPSHOT.jar包
    val dog1 = new Dog()
    dog1.setName("one")

    val dog2 = new Dog()
    dog2.setName("two")

    val dog3 = new Dog()
    dog3.setName("three")
  }

  private def buildDogs(dogOutputPath: String, hdfsMater: String, counts: RDD[(String, Int)]) = {
    // test executor class path , 依赖spark-rdd-1.0-SNAPSHOT.jar包
    val dogRDD = counts.mapPartitions { case iter =>
      val configFileStream = this.getClass.getClassLoader.getResourceAsStream("wordcount.properties")
      val properties = new Properties()
      properties.load(configFileStream)

      iter.map {case (word, count) =>
        val dog = new Dog()
        dog.setName(word)
        val maxCount = properties.getProperty("maxFavoriteNumber").toInt
        logger.info(s"maxFavoriteNumber from wordcount.properties is ${maxCount}")
        val finalNumber = if (count > maxCount) maxCount else count
        dog.setFavoriteNumber(finalNumber)
        // test --file
        val colorFromConf = properties.getProperty("dogFavoriteColor")
        logger.info(s"dogFavoriteColor from wordcount.properties is ${colorFromConf}")
        dog.setFavoriteColor(colorFromConf)
        dog
      }
    }

    deleteExistFile(dogOutputPath, hdfsMater)

    val configuration = counts.sparkContext.hadoopConfiguration

    // test package param
    AvroWriteSupport.setSchema(configuration, Dog.SCHEMA$)
    dogRDD.map((null, _)).saveAsNewAPIHadoopFile(dogOutputPath,
      classOf[Void], classOf[TrackerLog],
      classOf[AvroParquetOutputFormat[TrackerSession]], configuration)
  }

  private def deleteExistFile(outputPath: String, hdfsMater: String) = {
    val path = new Path(outputPath)

    val configuration = new Configuration()
    configuration.set("fs.defaultFS", hdfsMater)

    val fs = path.getFileSystem(configuration)

    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    path.toString
  }
}
