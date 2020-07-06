package com.twq.submit.kryo

import com.esotericsoftware.kryo.Kryo
import com.twq.spark.rdd.Dog
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by tangweiqun on 2017/9/15.
  */
class CustomKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Dog])
  }
}
