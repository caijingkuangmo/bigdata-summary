package com.twq.dataset.creation

import com.twq.dataset.Dog
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 记忆要领： 转RDD  .rdd; 转DataFrame  .toDF;   转DataSet  rdd是 toDS， DataFrame是as[T]
  */
object RDDDatasetTransform {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("RDDDatasetTransform")
      .getOrCreate()

    val dogs = Seq(Dog("jitty", "red"), Dog("mytty", "yellow"))

    val dogRDD = spark.sparkContext.parallelize(dogs)

    //1: RDD转DataFrame
    import spark.implicits._
    val dogDF = dogRDD.toDF()
    dogDF.show()

    val renameSchemaDF = dogRDD.toDF("first_name", "lovest_color")
    renameSchemaDF.show()

    //2: DataFrame转RDD, schema信息丢掉了
    val dogRowRDD: RDD[Row] = dogDF.rdd
    dogRowRDD.collect()
    renameSchemaDF.rdd.collect()

    //3: RDD转Dataset
    val dogDS = dogRDD.toDS()
    dogDS.show()

    //4: Dataset转RDD
    val dogRDDFromDs: RDD[Dog] = dogDS.rdd
    dogRDDFromDs.collect()

    //5: DataFrame转Dataset
    val dogDsFromDf = dogDF.as[Dog]
    dogDsFromDf.show()

    //6: Dataset转DataFrame
    val dogDfFromDs = dogDsFromDf.toDF()
    dogDfFromDs.show()

    spark.stop()
  }
}
