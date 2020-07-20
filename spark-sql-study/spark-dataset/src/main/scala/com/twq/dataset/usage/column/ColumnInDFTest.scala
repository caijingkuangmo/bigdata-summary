package com.twq.dataset.usage.column

import com.twq.dataset.Utils._
import org.apache.spark.sql.SparkSession

/**
  * Created by tangweiqun on 2017/10/11.
  */
object ColumnInDFTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ColumnInDFTest")
      .getOrCreate()

    val personDf = spark.read.json(s"${BASE_PATH}/people.json")

    personDf.select("name").show() //select name from ...

    //2：column的操作
      //2.1 获取所有的column
    personDf.columns

    //2.2 构建单个column
    val nameColumn = personDf.col("name")
    personDf.select(nameColumn).show()

    personDf("name")
    personDf.apply("name")
    personDf.select(personDf("name"), personDf.apply("name")).show()

    //$ - scala的构建column的简便符号
    import spark.implicits._
    $"name"
    personDf.select($"name").show()

    //从function中构建column
    import org.apache.spark.sql.functions._
    expr("age + 1")
    personDf.select(expr("age + 1")).show()

    lit("abc")
    personDf.select(lit("abc")).show()

    col("name")
    personDf.select(col("name")).show()

    column("name")
    personDf.select(column("name")).show()

    //2.2 操作columns
    personDf.withColumn("big_age", $"age").show
    personDf.withColumnRenamed("age", "big_age").show()
    personDf.drop("age").show()

    personDf.toDF("old_age", "first_name").show()


    spark.stop()

  }
}
