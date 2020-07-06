package com.laoliu

import java.net.URL
import java.util.UUID

import com.twq.spark.session.{TrackerLog, TrackerSession}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

class OneCookieTrackerLogsProcessor(trackLogs: Array[TrackerLog]) extends SessionGenerator {
  private val sordedTrackLogs = trackLogs.sortBy(_.getLogServerTime.toString)

  def buildSessions(domainLabelMap: Map[String,String], sessionCountAcc:LongAccumulator): List[TrackerSession] = {
    //1.会话切割
    val cutSessionResult = cutSessions(sordedTrackLogs)

    //2.生成会话
    cutSessionResult.map {case sessionLogs =>
      val session = new TrackerSession()
      session.setSessionId(UUID.randomUUID().toString)
      session.setSessionServerTime(sessionLogs.head.getLogServerTime)
      session.setCookie(sessionLogs.head.getCookie)
      session.setIp(sessionLogs.head.getIp)

      val pageviewLogs = sessionLogs.filter(_.getLogType.toString.equals("pageview"))
      if(pageviewLogs.length == 0) {
        session.setLandingUrl("-")
      } else {
        session.setLandingUrl(pageviewLogs.head.getUrl)
      }
      session.setPageviewCount(pageviewLogs.length)
      val clickLogs = sessionLogs.filter(_.getLogType.toString.equals("click"))
      session.setClickCount(clickLogs.length)
      if(pageviewLogs.length == 0) {
        session.setDomain("-")
      } else {
        val url = new URL(pageviewLogs.head.getUrl.toString)
        session.setDomain(url.getHost)
      }

      session.setDomainLabel(domainLabelMap.getOrElse(session.getDomain.toString, "-"))
      sessionCountAcc.add(1)
      session
    }
  }

}
