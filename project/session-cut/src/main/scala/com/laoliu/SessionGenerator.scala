package com.laoliu

import com.twq.spark.session.TrackerLog
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

trait SessionGenerator {
  private val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
    * 默认会话切割
    * 每隔30分钟就切割一次
    * @param sortedTrackerLogs  传入具体某个cookie下，所有的TrackerLog
    */
  def cutSessions(sortedTrackerLogs: Array[TrackerLog]): List[ArrayBuffer[TrackerLog]] = {
    /**
      * 会话切割传过来是 一个已经排好序的TrackerLog数组
      * 处理之后，你需要返回类似这样的结构 Seq(Seq(), Seq()),其中序列里的序列就是按照30分钟
      * 切割后 在同一会话下的日志
      */
    val cutedTrackerLogList = List.newBuilder[ArrayBuffer[TrackerLog]] //用于存放最后的日志切割结果
    val currentLogArray = new ArrayBuffer[TrackerLog]  // 用于存放 当前会话的所有日志
    sortedTrackerLogs.foldLeft((cutedTrackerLogList, Option.empty[TrackerLog])){
      case ((build, preLog),currLog) => {
        val currLogTime = dateFormat.parse(currLog.getLogServerTime.toString).getTime
        // 超过30分钟就生成一个新的会话
        if(preLog.nonEmpty &&
          currLogTime - dateFormat.parse(preLog.get.getLogServerTime.toString).getTime > 30 * 60 * 1000) {
          cutedTrackerLogList += currentLogArray.clone()
          currentLogArray.clear()
        }
        currentLogArray += currLog
        (build, Some(currLog))
      }
    }

    if(currentLogArray.nonEmpty) {
      cutedTrackerLogList += currentLogArray
    }
    cutedTrackerLogList.result()
  }
}


