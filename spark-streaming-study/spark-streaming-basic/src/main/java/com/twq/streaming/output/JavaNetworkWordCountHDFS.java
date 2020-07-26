package com.twq.streaming.output;

import com.twq.streaming.JavaLocalNetworkWordCount;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Created by tangweiqun on 2018/1/23.
 */
public class JavaNetworkWordCountHDFS {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        // StreamingContext 编程入口
        JavaStreamingContext ssc = new JavaStreamingContext("local[2]", "JavaLocalNetworkWordCount", Durations.seconds(1),
                System.getenv("SPARK_HOME"), JavaStreamingContext.jarOfClass(JavaLocalNetworkWordCount.class.getClass()));

        //数据接收器(Receiver)
        //创建一个接收器(JavaReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                "localhost", 9998, StorageLevels.MEMORY_AND_DISK_SER);

        //数据处理(Process)
        //处理的逻辑，就是简单的进行word count
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        //以文本的格式保存到HDFS上
        wordCounts.repartition(1).mapToPair(wordCount -> {
            Text text = new Text();
            text.set(wordCount.toString());
            return new Tuple2<>(NullWritable.get(), text);
        }).saveAsHadoopFiles("hdfs://master:9999/user/hadoop-twq/spark-course/streaming/data/hadoop/wordcount", "-hadoop",
                NullWritable.class, Text.class, TextOutputFormat.class);


        //以SEQUENCE的文件格式保存到HDFS上
        wordCounts.repartition(1).mapToPair(wordCount -> {
            Text text = new Text();
            text.set(wordCount.toString());
            return new Tuple2<>(NullWritable.get(), text);
        }).saveAsHadoopFiles("hdfs://master:9999/user/hadoop-twq/spark-course/streaming/data/hadoop/wordcount", "-hadoop",
                NullWritable.class, Text.class, SequenceFileOutputFormat.class);


        //注意：JAVA API没有saveAsTextFiles和saveAsObjectFiles，需要用上面的API保存到HDFS上
    }
}
