package com.twq.dataset.usage

import org.apache.spark.sql.DataFrame

/**
  * Created by tangweiqun on 2017/11/2.
  */
trait DFModule {

  def cal(personDf: DataFrame): DataFrame = {
    if (personDf.schema.contains("age")) {
      personDf.select("age")
    } else {
      personDf.select("name")
    }


  }

}
