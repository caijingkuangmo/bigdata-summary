#coding:utf-8

from pyspark import SparkContext, SparkConf
import time

def get_init_number(source):
    print "get init number from {0}, may be take much time........".format(source)
    time.sleep(1)
    return 1


if __name__ == "__main__":
    conf = SparkConf().setAppName("appName").setMaster("local")
    sc = SparkContext(conf=conf)

    parallelize_rdd = sc.parallelize([1, 2, 3, 3, 4], 2)
    """
    结果：[[1, 2], [3, 3, 4]]
    """
    print "parallelize_rdd = {0}".format(parallelize_rdd.glom().collect())

    map_rdd = parallelize_rdd.map(lambda x: x + 1)
    """
        结果：[[2, 3], [4, 4, 5]]
        """
    print "map_rdd = {0}".format(map_rdd.glom().collect())

    map_string_rdd = parallelize_rdd.map(lambda x: "{0}-{1}".format(x, "test"))
    """
        结果：[['1-test', '2-test'], ['3-test', '3-test', '4-test']]
        """
    print "map_string_rdd = {0}".format(map_string_rdd.glom().collect())

    flatmap_rdd = parallelize_rdd.flatMap(lambda x: range(x))
    """
        结果：[[0, 0, 1], [0, 1, 2, 0, 1, 2, 0, 1, 2, 3]]
        """
    print "flatmap_rdd = {0}".format(flatmap_rdd.glom().collect())

    filter_rdd = parallelize_rdd.filter(lambda x: x != 1)
    """
        结果：[[2], [3, 3, 4]]
        """
    print "filter_rdd = {0}".format(filter_rdd.glom().collect())

    glomRDD = parallelize_rdd.glom()
    """
    结果：[[1, 2], [3, 3, 4]]
    """
    print "glomRDD = {0}".format(glomRDD.collect())


    def map_partition_func(iterator):
        """
        每一个分区获取一次初始值，integerJavaRDD有两个分区，那么会调用两次getInitNumber方法
        所以对应需要初始化的比较耗时的操作，比如初始化数据库的连接等，一般都是用mapPartitions来为对每一个分区初始化一次，而不要去使用map操作
        :param iterator:
        :return:
        """
        init_number = get_init_number("map_partition_func")
        yield map(lambda x : x + init_number, iterator)
    map_partition_rdd = parallelize_rdd.mapPartitions(map_partition_func)
    """
        结果：[[[2, 3]], [[4, 4, 5]]]
        """
    print "map_partition_rdd = {0}".format(map_partition_rdd.glom().collect())

    def map_func(x):
        """
        遍历每一个元素的时候都会去获取初始值，这个integerJavaRDD含有5个元素，那么这个getInitNumber方法会被调用4次，严重的影响了时间，不如mapPartitions性能好
        :param x:
        :return:
        """
        init_number = get_init_number("map_func")
        return x + init_number
    map_rdd_init_number = parallelize_rdd.map(map_func)
    """
        结果：[[2, 3], [4, 4, 5]]
        """
    print "map_rdd_init_number = {0}".format(map_rdd_init_number.glom().collect())

    def map_partition_with_index_func(partition_index, iterator): yield (partition_index, sum(iterator))
    map_partition_with_index_rdd = parallelize_rdd.mapPartitionsWithIndex(map_partition_with_index_func)
    """
        结果：[[(0, 3)], [(1, 10)]]
        """
    print "map_partition_with_index_rdd = {0}".format(map_partition_with_index_rdd.glom().collect())

    print parallelize_rdd.getNumPartitions()

    distinct_rdd = parallelize_rdd.distinct()
    print "distinct_rdd = {0}".format(distinct_rdd.glom().collect())







