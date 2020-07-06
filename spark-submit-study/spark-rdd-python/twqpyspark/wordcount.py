from pyspark import SparkContext, SparkConf

import os
import shutil

if __name__ == "__main__":
    conf = SparkConf().setAppName("appName").setMaster("local")
    sc = SparkContext(conf=conf)

    sourceDataRDD = sc.textFile("file:///Users/tangweiqun/test.txt")

    wordsRDD = sourceDataRDD.flatMap(lambda line: line.split())

    keyValueWordsRDD = wordsRDD.map(lambda s: (s, 1))

    wordCountRDD = keyValueWordsRDD.reduceByKey(lambda a, b: a + b)

    outputPath = "/Users/tangweiqun/wordcount"
    if os.path.exists(outputPath):
        shutil.rmtree(outputPath)

    wordsRDD.saveAsTextFile("file://" + outputPath)

    print wordCountRDD.collect()