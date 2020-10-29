package com.twq.spark.rdd.sources

import com.esotericsoftware.kryo.Kryo
import com.twq.spark.rdd.Dog
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by tangweiqun on 2017/8/24.
  * spark-shell --master spark://master:7077 --jars parquet-avro-1.8.1.jar,spark-rdd-1.0-SNAPSHOT.jar,spark-avro_2.11-3.2.0.jar --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryo.registrator=com.twq.spark.rdd.sources.ParquetAvroWriterKryoRegistrator
  */
object ParquetAvroWriterApiTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("test")

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "com.twq.spark.rdd.sources.ParquetAvroWriterKryoRegistrator")

    val sc = new SparkContext(conf)

    val dog1 = Dog.newBuilder().setName("nani").setFavoriteNumber(3).setFavoriteColor("yellow").build()

    val dog2 = Dog.newBuilder().setName("kaji").setFavoriteNumber(4).setFavoriteColor("red").build()

    val dog3 = Dog.newBuilder().setName("kk").setFavoriteNumber(6).setFavoriteColor("black").build()

    val dataRdd = sc.parallelize(Seq(dog1, dog2, dog3), 2)

    dataRdd.mapPartitionsWithIndex { case (pid, iter) =>
      val parquetOutputPath = s"hdfs://master:9999/users/hadoop-twq/otherparquet/part-${pid}"
      val parquetWriter =
        AvroParquetWriter.builder[Dog](new Path(parquetOutputPath)).withSchema(Dog.SCHEMA$).build()
      iter.foreach(parquetWriter.write(_))

      parquetWriter.close()

      val avroOutputPath = new Path(s"hdfs://master:9999/users/hadoop-twq/otheravro/part-${pid}.avro")
      val config = new Configuration()
      val outputStream = avroOutputPath.getFileSystem(config).create(avroOutputPath)
      val userDatumWriter = new SpecificDatumWriter[Dog](classOf[Dog])
      val dataFileWriter = new DataFileWriter[Dog](userDatumWriter)
      dataFileWriter.create(Dog.SCHEMA$, outputStream)
      iter.foreach(dataFileWriter.append(_))

      dataFileWriter.close()

      Iterator()
    }.foreach((_: Nothing) => ())
  }

}

class ParquetAvroWriterKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Dog])
  }
}

