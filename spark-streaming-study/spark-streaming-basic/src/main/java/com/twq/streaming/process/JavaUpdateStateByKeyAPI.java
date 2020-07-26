/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twq.streaming.process;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9998`
 * and then run the example
  spark-submit --class com.twq.checkpoint.JavaUpdateStateByKeyAPI \
  --master spark://master:7077 \
  --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 512m \
  --total-executor-cores 4 \
  --executor-cores 2 \
  /home/hadoop-twq/spark-course/streaming/streaming-1.0-SNAPSHOT.jar master 9998
 */
public class JavaUpdateStateByKeyAPI {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: JavaMapWithStateAPI <hostname> <port>");
            System.exit(1);
        }

        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("JavaUpdateStateByKeyAPI");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        ssc.checkpoint("hdfs://master:9999/spark-streaming/updatestatebykey");

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER_2);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

        JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(s -> new Tuple2<>(s, 1));

        // Update the cumulative count function
        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
                (values, state) -> {
                    int out = 0;
                    if (state.isPresent()) {
                        out = out + state.get();
                    }
                    for (Integer v : values) {
                        out = out + v;
                    }
                    return Optional.of(out);
                };

        // DStream made of get cumulative counts that get updated in every batch
        JavaPairDStream<String, Integer> runningCounts = wordsDstream.updateStateByKey(updateFunction);

        runningCounts.print();
        ssc.start();
        ssc.awaitTermination();
    }
}
