package com.twq.dataset.datasource

import com.twq.dataset.Utils._
import org.apache.spark.sql.SparkSession

/**
  * Created by tangweiqun on 2017/10/11.
  */
object OrcFileTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OrcFileTest")
      .getOrCreate()

    //1: 将json文件数据转化成orc文件数据
    val df = spark.read.json(s"${BASE_PATH}/people.json")
    df.show()

    df.write.option("compression", "snappy").orc(s"${BASE_PATH}/orc")

    val orcFileDF = spark.read.orc(s"${BASE_PATH}/orc")
    orcFileDF.show()

    spark.stop()
  }
}
