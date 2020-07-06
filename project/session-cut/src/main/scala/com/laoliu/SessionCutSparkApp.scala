package com.laoliu

import com.twq.spark.session.{TrackerLog, TrackerSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 上spark环境运行前，需要关注这么几个方面：
  * 1.适配读取文件路径
  * 2.适配输出目录
  * 3.设置在spark运行
  * 4.运行时的参数适配
  * 5.打包jar
  *   (1).默认打包是不会把依赖打进去，比如这里依赖org.apache.parquet，你需要到配置的repo
  *   repo\org\apache\parquet\parquet-avro\1.8.1下把依赖包上传，通过spark命令 --jars 引入
spark-submit  --class com.twq.session.SessionCutETL  \
	--master spark://master:7077 \
  --deploy-mode client \
	--driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  --total-executor-cores 2 \
  --jars parquet-avro-1.8.1.jar \
	--conf spark.sessioncut.visitLogsInputPath=hdfs://master:9999/user/hadoop-twq/example/rawdata/visit_log.txt \
  --conf spark.sessioncut.cookieLabelInputPath=hdfs://master:9999/user/hadoop-twq/example/cookie_label.txt \
  --conf spark.sessioncut.baseOutputPath=hdfs://master:9999/user/hadoop-twq/example/output \
  spark-sessioncut-1.0-SNAPSHOT.jar text
  *   (2).当然还可以通过插件 maven-assembly-plugin 把依赖打进来，这样spark命令调用时，就不用通过 --jars引用
  *   另外spark-core_2.11依赖在spark应用是默认带上的，所以可以在 打包时给这个加上<scope>provided</scope>，这样
  *   就不会把这个依赖打进去，但是本地运行时，需要注释这个，否则无法正常运行
 spark-submit  --class com.laoliu.SessionCutSparkApp  \
	--master spark://master:7077 \
  --deploy-mode client \
	--driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  --total-executor-cores 2 \
	--conf spark.sessioncut.visitLogsInputPath=hdfs://master:9999/user/hadoop-laoliu/example/rawdata/visit_log.txt \
  --conf spark.sessioncut.cookieLabelInputPath=hdfs://master:9999/user/hadoop-laoliu/example/cookie_label.txt \
  --conf spark.sessioncut.baseOutputPath=hdfs://master:9999/user/hadoop-laoliu/example/output \
  sessioncut-1.0-SNAPSHOT-jar-with-dependencies.jar parquet
  */

object SessionCutSparkApp {
  val filterLogType =Seq("pageview", "click")

  def main(args: Array[String]): Unit = {
    val fileType = if (args.nonEmpty) args(0) else "parquet"

    val conf = new SparkConf()
    conf.setAppName("SessionCutSparkApp")
    // 设置本地运行  并适配spark运行
    if(!conf.contains("spark.master"))
      conf.setMaster("local")

    // 开启kryo序列化，因为在读取日志进行RDD操作时，可能设置序列化操作
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val domainLabelMap = Map("www.baidu.com" -> "level1", "www.ali.com" -> "level2",
      "jd.com" -> "level3", "youku.com" -> "level4")
    val domainLabelMapB = sc.broadcast(domainLabelMap)

    // 用于统计会话个数
    val sessionCountAcc = sc.longAccumulator("session-count")

    // 读取网站日志记录
//    val baseDir = "data/rawdata"
//    val outputDir = "data/output"
//    val visitLogPath = s"${baseDir}/visit_log.txt"
//    val cookieLabelPath = "data/cookie_label.txt"

    val baseDir = "data"
    val visitLogPath = conf.get("spark.sessioncut.visitLogsInputPath", s"${baseDir}/rawdata/visit_log.txt")
    val cookieLabelPath = conf.get("spark.sessioncut.cookieLabelInputPath", s"${baseDir}/cookie_label.txt")
    val outputDir = conf.get("spark.sessioncut.baseOutputPath", s"${baseDir}/output")

    val logLineRDD = sc.textFile(visitLogPath)
    // RDD转换操作
    // 1.解析成网站日志对象
    val trackerLogRDD:RDD[TrackerLog] = logLineRDD.flatMap(RawLogParser.oneTrackerLogParser(_))
    //  缓存trackerLog
    trackerLogRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // 2.过滤脏数据
    val filterTrackerLogRDD:RDD[TrackerLog] = trackerLogRDD.filter(log => filterLogType.contains(log.getLogType))
    // 3.按cookie进行分组
    val groupByCookieLogRDD:RDD[(String, Iterable[TrackerLog])] = filterTrackerLogRDD.groupBy(_.getCookie.toString)
    // 4.会话切割
    // iter 类似CompactBuffer({},{})
    val sessionsRDD:RDD[(String,TrackerSession)] = groupByCookieLogRDD.flatMapValues(iter =>{
      val processor = new OneCookieTrackerLogsProcessor(iter.toArray)
      processor.buildSessions(domainLabelMapB.value, sessionCountAcc)
    })
    // 5.打标签
    val cookieLabelRDD:RDD[(String,String)] = sc.textFile(cookieLabelPath).map {case line =>
      val cookieLabelItems = line.trim.split("\\|")
      (cookieLabelItems(0), cookieLabelItems(1))
    }

    val joinedCookieLabelSessionRDD:RDD[(String,TrackerSession)] = sessionsRDD.leftOuterJoin(cookieLabelRDD).mapValues {
      case (session, cookieLabelOpt) =>
        if(cookieLabelOpt.nonEmpty)
          session.setCookieLabel(cookieLabelOpt.get)
        else
          session.setCookieLabel("-")
        session
    }

    //把日志和会话结果 输出到文件里
    OutputComponent.outputByFileType(fileType).writeOutputData(sc,outputDir,trackerLogRDD,joinedCookieLabelSessionRDD.values)
//    joinedCookieLabelSessionRDD.values.collect().foreach(println)
    sc.stop()
  }
}
