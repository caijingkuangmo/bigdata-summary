
practice:
	simple: 和hbase的基本交互逻辑(Dirver, Executor)
	
	HBaseConnectionCache.scala：Hbase连接池
	
	HBaseContext.scala：抽离出和HBase交互操作进行封装，主要涉及Hbase的增删改查操作，
		让我们不用太关注连接的逻辑，更多关注业务实现细节，可以作为scala语言应用的研究案例， 
		应用示例代码：usage/IngestionData2HbaseWithPut(插入逻辑优化版本：usage/IngestionData2HbaseWithPutOpt)
	
	HBase查询结果和RDD结合：usage/HBaseCounter
	
	HBase和Solr构建OLAP平台：
		CSV数据生成：storage\CSVGenerator.java
		CSV数据转换成HFile：storage\ConvertToHFile.java(基于MapReduce实现)
			是基于bulk load实现导入数据到hbase，这种速度会更快
			hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /user/hadoop-twq/hbase-course/hfiles/20180506 sensor
		
		查看插入数据：storage\Validation.java
		
		Solr主要帮我们建立二级索引，加快hbase过滤查询
			获取hbase的数据转换成索引：storage\Spark2SolrIndexer.scala
			通过Solr二级索引查询数据：storage\DataRetrieval.java

project：
	omneo-java-web： spring-boot web项目demo
	map-tile： 地图切片项目，结合hbase存储，父子项目版本管理等
	HBase和Solr构建OLAP真实数据量和资源.txt：面试时资源使用问题回答
	ppt：地图切片项目逻辑