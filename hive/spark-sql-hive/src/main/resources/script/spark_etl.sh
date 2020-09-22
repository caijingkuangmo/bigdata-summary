#!/usr/bin/env bash

spark-submit  \
	--class com.twq.spark.rdd.example.ClickTrackerEtl  \
	--master spark://master:7077 \
	--deploy-mode client \
	--driver-memory 1g \
	--executor-memory 1g \
	--num-executors 2 \
	--jars parquet-avro-1.8.1.jar \
	--conf spark.session.groupBy.numPartitions=2 \
	--conf spark.tracker.trackerDataPath=hdfs://master:9999/user/hadoop-twq/example/ \
	spark-rdd-1.0-SNAPSHOT.jar \
	nonLocal