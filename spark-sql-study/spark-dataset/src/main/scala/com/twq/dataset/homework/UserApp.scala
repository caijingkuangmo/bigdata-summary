package com.twq.dataset.homework

import org.apache.spark.sql.SparkSession

case class User(
                 userId:Int,
                 gender:String,
                 age:Int,
                 occupation:String,
                 zipcode:String
               )

object UserApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("UserApp")
      .master("local")
      .getOrCreate()

    val sourcePath = "src/main/resources/homework/user.txt"
    // 1.生成User类型RDD
    val userRDD = spark.sparkContext.textFile(sourcePath).map(
      line => {
        val userArr = line.split("::")
        User(userArr(0).toInt, userArr(1), userArr(2).toInt, userArr(3), userArr(4))
      }
    )

    // 2.将RDD转成DataFrame，在转成DataSet
    import spark.implicits._
    val userDf = userRDD.toDF()  //第一种 直接调用转换接口， 前提要导入隐式转换
    val userDf2 = spark.createDataFrame(userRDD)  //第二种 创建接口的方式
    userDf.as[User]  //转换成DataSet  as的方式
    spark.createDataset(userDf2.rdd)

    //3.将RDD转成DataSet
    userRDD.toDS()
    spark.createDataset(userRDD)

    spark.stop()
  }
}
