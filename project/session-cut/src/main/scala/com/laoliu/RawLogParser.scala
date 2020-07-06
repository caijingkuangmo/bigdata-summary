package com.laoliu

import com.twq.spark.session.TrackerLog

object RawLogParser {
  def oneTrackerLogParser(line:String): Option[TrackerLog] = {
    if (line.contains("#type")) {
      None
    } else {
      val logItemArr = line.split("\\|")
      val trackerLog = new TrackerLog()
      trackerLog.setLogType(logItemArr(0))
      trackerLog.setLogServerTime(logItemArr(1))
      trackerLog.setCookie(logItemArr(2))
      trackerLog.setIp(logItemArr(3))
      trackerLog.setUrl(logItemArr(4))
      Some(trackerLog)
    }

  }
}
