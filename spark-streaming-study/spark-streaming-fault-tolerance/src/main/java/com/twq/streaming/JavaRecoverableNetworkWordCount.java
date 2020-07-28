
package com.twq.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.File;
import java.util.Arrays;
import java.util.regex.Pattern;


public final class JavaRecoverableNetworkWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    private static JavaStreamingContext createContext(String ip,
                                                      int port,
                                                      String checkpointDirectory,
                                                      String outputPath) {

        // If you do not see this printed, that means the StreamingContext has been loaded
        // from the new checkpoint
        System.out.println("Creating new context");
        File outputFile = new File(outputPath);
        if (outputFile.exists()) {
            outputFile.delete();
        }
        SparkConf sparkConf = new SparkConf().setAppName("JavaRecoverableNetworkWordCount");
        // Create the context with a 1 second batch size
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        ssc.checkpoint(checkpointDirectory);

        // Create a socket stream on target ip:port and count the
        // words in input stream of \n delimited text (eg. generated by 'nc')
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(ip, port);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        words.foreachRDD((rdd, time) -> {

        });

        return ssc;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("You arguments were " + Arrays.asList(args));
            System.err.println(
                    "Usage: JavaRecoverableNetworkWordCount <hostname> <port> <checkpoint-directory>\n" +
                            "     <output-file>. <hostname> and <port> describe the TCP server that Spark\n" +
                            "     Streaming would connect to receive data. <checkpoint-directory> directory to\n" +
                            "     HDFS-compatible file system which checkpoint data <output-file> file to which\n" +
                            "     the word counts will be appended\n" +
                            "\n" +
                            "In local mode, <master> should be 'local[n]' with n > 1\n" +
                            "Both <checkpoint-directory> and <output-file> must be absolute paths");
            System.exit(1);
        }

        String ip = args[0];
        int port = Integer.parseInt(args[1]);
        String checkpointDirectory = args[2];
        String outputPath = args[3];

        // Function to create JavaStreamingContext without any output operations
        // (used to detect the new context)
        Function0<JavaStreamingContext> createContextFunc =
                () -> createContext(ip, port, checkpointDirectory, outputPath);

        JavaStreamingContext ssc =
                JavaStreamingContext.getOrCreate(checkpointDirectory, createContextFunc);
        ssc.start();
        ssc.awaitTermination();
    }
}