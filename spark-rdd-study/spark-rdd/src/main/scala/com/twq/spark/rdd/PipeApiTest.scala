package com.twq.spark.rdd

import java.io._

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

/**
  * Created by tangweiqun on 2017/8/24.
  */
object PipeApiTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test")

    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(List("hi", "hello", "how", "are", "you"), 2)

    //运行进程需要的环境变量
    val env = Map("env" -> "test-env")
    //在执行一个分区task且处理分区输入元素之前将这个分区的全局数据作为脚本的输入的话，则需要定义这个函数
    // 上面这句话 我没理解 但是这里的实现效果 就是在每个分区的数据前 加了this is task context data per partition
    def printPipeContext(func: String => Unit): Unit = {
      val taskContextData = "this is task context data per partition"
      func(taskContextData)
    }
    //在执行分区task的时候，需要对每一个输入元素做特殊处理的话，可以定义这个函数参数
    def printRDDElement(ele: String, func: String => Unit): Unit = {
      if (ele == "hello") {
        func("dog")
      } else func(ele)
    }
    //表示执行一个本地脚本(可以是shell，python，java等各种能通过java的Process启动起来的脚本进程)
    //dataRDD的数据就是脚本的输入数据，脚本的输出数据会生成一个RDD即pipeRDD
    val pipeRDD = dataRDD.pipe(Seq("python", "/home/hadoop-twq/spark-course/echo.py"),
      env, printPipeContext, printRDDElement, false)

    pipeRDD.glom().collect()

    val pipeRDD2 = dataRDD.pipe("sh /home/hadoop-twq/spark-course/echo.sh")
    pipeRDD2.glom().collect()



    // 你的python脚本所在的hdfs上目录
    // 然后将python目录中的文件代码内容全部拿到
    val scriptsFilesContent =
      sc.wholeTextFiles("hdfs://master:9999/users/hadoop-twq/pipe").collect()
    // 将所有的代码内容广播到每一台机器上
    val scriptsFilesB = sc.broadcast(scriptsFilesContent)
    // 创建一个数据源RDD
    val dataRDDTmp = sc.parallelize(List("hi", "hello", "how", "are", "you"), 2)
    // 将广播中的代码内容写到每一台机器上的本地文件中
    dataRDDTmp.foreachPartition(_ => {
      scriptsFilesB.value.foreach { case (filePath, content) =>
        val fileName = filePath.substring(filePath.lastIndexOf("/") + 1)
        val file = new File(s"/home/hadoop-twq/spark-course/pipe/${fileName}")
        if (!file.exists()) {
          val buffer = new BufferedWriter(new FileWriter(file))
          buffer.write(content)
          buffer.close()
        }
      }
    })
    // 对数据源rdd调用pipe以分布式的执行我们定义的python脚本
    val hdfsPipeRDD = dataRDDTmp.pipe(s"python /home/hadoop-twq/spark-course/pipe/echo_hdfs.py")

    hdfsPipeRDD.glom().collect()


    //我们不能用如下的方法来达到将hdfs上的文件分发到每一个executor上
    sc.addFile("hdfs://master:9999/users/hadoop-twq/pipe/echo_hdfs.py")
    SparkFiles.get("echo.py")

  }

}
