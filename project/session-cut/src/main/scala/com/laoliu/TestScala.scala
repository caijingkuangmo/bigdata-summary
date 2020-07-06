package com.laoliu

import scala.collection.mutable.ArrayBuffer

object TestScala {
  def main(args: Array[String]): Unit = {
    //test Builder
    // 用在不变集合进行扩展的时候，Builder提供 +=方法扩展， result方法获取结果
//    val arrayBufferBuilder:scala.collection.mutable.Builder[String, ArrayBuffer[String]] =
//      ArrayBuffer.newBuilder[String]
//    arrayBufferBuilder += "one"
//    arrayBufferBuilder += "two"
//    arrayBufferBuilder += "three"
//    val result:ArrayBuffer[String] = arrayBufferBuilder.result()
//    println(result)
//
//    val arrayBuffer = new ArrayBuffer[String]
//    arrayBuffer += "1"
//    arrayBuffer += "2"
//    arrayBuffer += "3"
//    println(arrayBuffer)
//
//    val seqBuilder = Seq.newBuilder[String]
//    seqBuilder += "haha"
//    seqBuilder += "heihei"
//    seqBuilder += "hehe"
//    val ret:Seq[String] = seqBuilder.result()
//    println(ret)

      //除了元组支持这样的解构赋值，其他的都不支持
//    val line = "pageview|2017-09-04 12:00:00|cookie1|127.0.0.3|https://www.baidu.com"
//    val (logType, serverTime, cookie, ip, url) = line.split("\\|")

//    line.split("\\|").toTraversable match {
//      case (logType, serverTime, cookie, ip, url) => println(logType)
//    }


  }
}
