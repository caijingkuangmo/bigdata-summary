package com.twq.streaming.output;

import com.twq.streaming.JavaLocalNetworkWordCount;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by tangweiqun on 2018/1/23.
 */
public class JavaNetworkWordCountForeachRDDDataFrame {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws InterruptedException {
        // StreamingContext 编程入口
        JavaStreamingContext ssc = new JavaStreamingContext("local[2]", "JavaLocalNetworkWordCount", Durations.seconds(1),
                System.getenv("SPARK_HOME"), JavaStreamingContext.jarOfClass(JavaLocalNetworkWordCount.class.getClass()));

        //数据接收器(Receiver)
        //创建一个接收器(JavaReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                "localhost", 9998, StorageLevels.MEMORY_AND_DISK_SER);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

        //用SPARK SQL来进行WORD COUNT
        words.foreachRDD(rdd -> {
            SparkSession spark = SparkSession.builder().config(rdd.rdd().sparkContext().getConf()).getOrCreate();
            JavaRDD<Row> wordRowRDD = rdd.map(word -> RowFactory.create(word));
            List<StructField> fields = Arrays.asList(DataTypes.createStructField("word", DataTypes.StringType, true));
            StructType schema = DataTypes.createStructType(fields);

            Dataset<Row> wordsDataFrame = spark.createDataFrame(wordRowRDD, schema);
            wordsDataFrame.createOrReplaceTempView("words");

            Dataset<Row> wordCountsDataFrame =
                    spark.sql("select word, count(*) as total from words group by word");
            wordCountsDataFrame.show();

        });

        //将word count time写入到parquet中
        words.foreachRDD((rdd, time) -> {
            SparkSession spark = SparkSession.builder().config(rdd.rdd().sparkContext().getConf()).getOrCreate();
            JavaRDD<Row> wordRowRDD = rdd.map(word -> RowFactory.create(word));
            List<StructField> fields = Arrays.asList(DataTypes.createStructField("word", DataTypes.StringType, true));
            StructType schema = DataTypes.createStructType(fields);

            Dataset<Row> wordsDataFrame = spark.createDataFrame(wordRowRDD, schema);

            Dataset<Row> wordCountsDataFrame = wordsDataFrame.groupBy(wordsDataFrame.col("word")).count();

            JavaRDD<Row> wordCountsWithTime =
                    wordCountsDataFrame.javaRDD().map(row -> RowFactory.create(row.get(0), row.get(1), time.milliseconds()));

            List<StructField> finalFields = Arrays.asList(DataTypes.createStructField("word", DataTypes.StringType, true),
                    DataTypes.createStructField("count", DataTypes.IntegerType, true),
                    DataTypes.createStructField("ts", DataTypes.LongType, true));
            StructType finalSchema = DataTypes.createStructType(finalFields);

            spark.createDataFrame(wordCountsWithTime,finalSchema)
                    .write()
                    .mode(SaveMode.Append)
                    .parquet("hdfs://master:9999/user/hadoop-twq/spark-course/streaming/parquet");


        });

        //等待Streaming程序终止
        ssc.awaitTermination();

    }
}
