package com.twq.dataskew

import java.util.concurrent.ThreadLocalRandom

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * groupByKey存在OOM异常
  * 解决方案：采用两阶段聚合操作
  * 两阶段聚合可以解决的一些常见：
  *   1. 聚合操作中存储的OOM异常
  *   2. 聚合操作中存在的数据倾斜问题
  */
object TwoPhaseGroupTopN {
  def main(args: Array[String]): Unit = {
    val n = 3
    runningSparkJob(createSparkContext, sparkContext => {
      val lines = sparkContext.textFile("data/grouptopn/test.txt")
      val kvRDD = lines.map(line => {
        val temp = line.split(",")
        (temp(0), temp(1).toInt)
      })
      val random = ThreadLocalRandom.current()
      val randomKeyRDD = kvRDD.map { case (key, value) =>
        // 第一阶段第一步：在key前加一个随机数
        ((random.nextInt(24), key), value)
      }
      val groupPhaseOne = randomKeyRDD.groupByKey() // 第一次聚合操作, 带有随机数key
      val groupTopNPhaseOne = groupPhaseOne.flatMap { case (key, iter) =>
        // key : (3,java)
        val course = key._2
        // 2,1,3,4,7,5 -> 1,2,3,4,5,7 -> 4,5,7 -> 7,5,4
        val topN = iter.toList.sorted.takeRight(n).reverse
        // java 7
        // java 5
        // java 4
        topN.map((course, _))
      }
      val groupPhaseTwo = groupTopNPhaseOne.groupByKey() // 第二次聚合操作，针对原始的key
      val groupTopNPhaseTwo = groupPhaseTwo.map { case (key, iter) =>
        // key : java
        // value :
        (key, iter.toList.sorted.takeRight(n).reverse)
      }
      FileSystem.get(sparkContext.hadoopConfiguration).delete(new Path("result/grouptopn"), true)
      groupTopNPhaseTwo.saveAsTextFile("result/grouptopn")
    })
  }

  /**
    * 创建一个SparkContext
    */
  def createSparkContext = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("mock-topn")
    SparkContext.getOrCreate(conf)
  }

  /**
    * 运行spark程序
    *
    * @param createSparkContext
    * @param operator
    * @param closeSparkContext
    */
  def runningSparkJob(createSparkContext: => SparkContext, operator: SparkContext => Unit,
                      closeSparkContext: Boolean = false): Unit = {
    // 创建上下文
    val sc = createSparkContext
    // 执行并在执行后关闭上下文
    try operator(sc)
    finally if (closeSparkContext) sc.stop()
  }
}
