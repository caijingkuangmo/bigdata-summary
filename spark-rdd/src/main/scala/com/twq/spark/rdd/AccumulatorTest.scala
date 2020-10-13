package com.twq.spark.rdd

import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiConsumer

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangweiqun on 2017/9/3.
  */
object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test")

    val sc = new SparkContext(conf)

    val longAccumulator = sc.longAccumulator("count mapped data")
    val collectionAccumulator = sc.collectionAccumulator[String]("collect mapped data")
    val mapAccumulator = new CustomAccumulator
    sc.register(mapAccumulator)  //自定义的 需要进行注册

    val logData = sc.parallelize(Seq("plane", "fish", "duck", "dirty", "people", "plane"), 2)
    // 上面代码在driver端执行
    logData.foreach(str => {  // 这里就在Executor端执行
      if (str == "plane") {
        longAccumulator.add(1L)
      }
      try {
        // some code
      } catch {
        case e: Exception => {
          collectionAccumulator.add(e.getMessage)   //这里实例了 统计task发生异常信息
        }
      }

      mapAccumulator.add(str)
    })

    longAccumulator.sum // 6
    collectionAccumulator.value // "plane", "fish", "duck", "dirty", "people", "plane"
    mapAccumulator.value //"plane -> 2", "fish -> 1", "duck -> 1", "dirty -> 1", "people -> 1",
  }
}

class CustomAccumulator extends AccumulatorV2[String, ConcurrentHashMap[String, Int]] {

  private val map = new ConcurrentHashMap[String, Int]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, ConcurrentHashMap[String, Int]] = {
    val newAcc = new CustomAccumulator()
    newAcc.map.putAll(map)
    newAcc
  }

  override def reset(): Unit = map.clear()
  // 在task端执行
  override def add(v: String): Unit = {
    map.synchronized {
      if (map.containsKey(v)) {
        map.put(v, map.get(v) + 1)
      } else map.put(v, 1)
    }
  }
  //在 driver端执行
  override def merge(other: AccumulatorV2[String, ConcurrentHashMap[String, Int]]): Unit = other match {
    case o: CustomAccumulator => {
      o.map.forEach(new BiConsumer[String, Int] {
        override def accept(key: String, value: Int): Unit = {
          if (map.containsKey(key)) {
            map.put(key, map.get(key) + value)
          } else {
            map.put(key, value)
          }
        }
      })
    }
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: ConcurrentHashMap[String, Int] = map
}
