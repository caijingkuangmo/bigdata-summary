from pyspark import SparkContext, SparkConf
import utils
import output_path_service

if __name__ == "__main__":
    """
    export HADOOP_CONF_DIR=/home/hadoop-twq/hadoop-2.6.5/etc/hadoop
    spark-submit \
    --name "PythonWordCount" \
    --master yarn \
    --deploy-mode client \
    --driver-memory 512m \
    --executor-memory 512m \
    --num-executors 2 \
    --executor-cores 1 \
    --py-files /home/hadoop-twq/spark-course/word_count_python/output_path_service.py,/home/hadoop-twq/spark-course/word_count_python/utils.py \
    /home/hadoop-twq/spark-course/spark_word_count.py 
    
    spark-submit \
    --name "PythonWordCount" \
    --master yarn \
    --deploy-mode client \
    --driver-memory 512m \
    --executor-memory 512m \
    --num-executors 2 \
    --executor-cores 1 \
    --py-files word_count_python.zip \
    /home/hadoop-twq/spark-course/spark_word_count.py
    """
    conf = SparkConf().setAppName("appName")
    sc = SparkContext(conf=conf)

    sourceDataRDD = sc.textFile("hdfs://master:9999/users/hadoop-twq/word.txt")

    wordsRDD = sourceDataRDD.flatMap(lambda line: line.split())

    keyValueWordsRDD = wordsRDD.map(lambda s: (s, 1))

    wordCountRDD = keyValueWordsRDD.reduceByKey(lambda a, b: a + b)

    wordCountRDD.saveAsTextFile("hdfs://master:9999" + output_path_service.get_output_path())

    print utils.get_rdd_result("wordCountRDD", wordCountRDD)