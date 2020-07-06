package com.laoliu

import com.twq.spark.session.{TrackerLog, TrackerSession}
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * spark程序运行结果输出组件
  * 主要分 输出文件类型：text parquet
  * 其中spark运行时，如果输出文件名已经存在，会报错退出，需要在输出前把已经存在的输出文件删除
  * 输出结果主要分 会话日志，原始日志
  */
trait OutputComponent {
  def writeOutputData(sc:SparkContext, baseOutputPath:String,
                      trackerLogRDD:RDD[TrackerLog],
                      joinedCookieLabelSessionRDD:RDD[TrackerSession]): Unit = {
    deleteIfExists(sc, baseOutputPath)
  }

  private def deleteIfExists(sc:SparkContext, baseOutputPath:String): Unit = {
    val path = new Path(baseOutputPath)
    val fileSystem = path.getFileSystem(sc.hadoopConfiguration)
    if(fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
  }
}

object OutputComponent {
  def outputByFileType(fileType: String) = {
    if("text".equals(fileType)) {
      new TextFileOutput
    } else {
      new ParquetFileOutput
    }
  }
}

/**
  * 写Parquet
  * 注意需要在pom引入org.apache.parquet
  */
class ParquetFileOutput extends OutputComponent {
  override def writeOutputData(sc: SparkContext, baseOutputPath: String, trackerLogRDD: RDD[TrackerLog], joinedCookieLabelSessionRDD: RDD[TrackerSession]): Unit = {
    super.writeOutputData(sc, baseOutputPath, trackerLogRDD, joinedCookieLabelSessionRDD)
    // 保存TrackerLog
    val trackerLogOutputPath = s"${baseOutputPath}/trackerLog"
    // 写parquet 依赖avro格式的schema，所以需要先设置avro schema
    AvroWriteSupport.setSchema(sc.hadoopConfiguration, TrackerLog.SCHEMA$)
    // 并且最新的写parquet接口，必须是key-value格式
    // 需要传入 输出路径，输出key类，输出value类，输出文件格式类
    trackerLogRDD.map((null,_)).saveAsNewAPIHadoopFile(trackerLogOutputPath,
      classOf[Void], classOf[TrackerLog], classOf[AvroParquetOutputFormat[TrackerLog]])

    //保存Session
    val trackerSessionOutputPath = s"${baseOutputPath}/trackerSession"
    AvroWriteSupport.setSchema(sc.hadoopConfiguration, TrackerSession.SCHEMA$)
    joinedCookieLabelSessionRDD.map((null, _)).saveAsNewAPIHadoopFile(trackerSessionOutputPath,
      classOf[Void], classOf[TrackerSession], classOf[AvroParquetOutputFormat[TrackerSession]])

  }
}

class TextFileOutput extends OutputComponent {
  override def writeOutputData(sc: SparkContext, baseOutputPath: String, trackerLogRDD: RDD[TrackerLog], joinedCookieLabelSessionRDD: RDD[TrackerSession]): Unit = {
    super.writeOutputData(sc, baseOutputPath, trackerLogRDD, joinedCookieLabelSessionRDD)
    // 保存TrackerLog
    val trackerLogOutputPath = s"${baseOutputPath}/trackerLog"
    trackerLogRDD.saveAsTextFile(trackerLogOutputPath)

    //保存Session
    val trackerSessionOutputPath = s"${baseOutputPath}/trackerSession"
    joinedCookieLabelSessionRDD.saveAsTextFile(trackerSessionOutputPath)
  }

}
