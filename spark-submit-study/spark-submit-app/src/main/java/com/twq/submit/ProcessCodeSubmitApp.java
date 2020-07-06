package com.twq.submit;

import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.io.IOException;

/**
 * Created by tangweiqun on 2017/9/16.
 */
public class ProcessCodeSubmitApp {
    public static void main(String[] args) throws InterruptedException {
        try {
            Process process = new SparkLauncher()
                    .setAppResource("/Users/tangweiqun/spark/source/spark-course/spark-submit-app/target/spark-submit-app-1.0-SNAPSHOT.jar")
                    .setMainClass("com.twq.submit.LocalSparkTest")
                    .setAppName("test code launch")
                    .setMaster("yarn")
                    .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                    .redirectError()
                    .redirectOutput(new File("/Users/tangweiqun/spark-course/output.txt"))
                    .launch();

            System.out.println("started app");

            process.waitFor();

            System.out.println("结束");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
