import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object DuplicateRemovalETL {
  def main(args: Array[String]): Unit = {
    val intputPath = "data/rawdata/input.csv"
    val orderPath = "data/rawdata/order.csv"

    // 一、机顶盒信息去重
    runningSparkJob(createSparkContent, sc => {
      val userSetTopBoxInfoRDD:RDD[UserSetTopBoxInfo] = sc.textFile(intputPath)
        .filter(!_.isEmpty)
        .map {case line => UserSetTopBoxInfo.parseLine(line)}
      // 按user id分组
      val groupedByUserIdRDD:RDD[(String, Iterable[UserSetTopBoxInfo])] = userSetTopBoxInfoRDD
          .map(info => (info.userId, info))
          .groupByKey()
      // 整合去重
      val duplicateedRDD:RDD[UserSetTopBoxInfo] = groupedByUserIdRDD.map { case (userId, iter) => {
        val topOneUserTypeInfo = iter.toList.sortBy(_.userType).reverse.head  //取出userType最大的一项
        val topOneSetTopBoxInfo = iter.toList.sortBy(_.setTopBoxType).reverse.head  //取出setTopBoxType最大的一项
        new UserSetTopBoxInfo(userId,
          topOneUserTypeInfo.cityId,
          topOneUserTypeInfo.createTime,
          topOneSetTopBoxInfo.setTopBoxId,
          topOneUserTypeInfo.productCode,
          topOneUserTypeInfo.userType,
          topOneSetTopBoxInfo.setTopBoxType
        )
      }}
//      duplicateedRDD.collect().foreach(println)
    })


    // 二、 订单信息去重
    runningSparkJob(createSparkContent, sc => {
      val orderInfoRDD:RDD[OrderInfo] = sc.textFile(orderPath)
        .filter(!_.isEmpty)
        .map {case line => OrderInfo.parseLine(line)}

      // 按order id分组
      val groupedByOrderIdRDD:RDD[(String, Iterable[OrderInfo])] = orderInfoRDD.groupBy(_.orderId)
      // 整合去重
      val duplicateedRDD:RDD[OrderInfo] = groupedByOrderIdRDD.map { case (orderId, iter) => {
        iter.maxBy(_.modifyTime)
//        iter.toList.sortBy(_.modifyTime).reverse.head  //取出修改时间最大的那向
      }}
      duplicateedRDD.collect().foreach(println)
    },true)
  }

  def createSparkContent = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DuplicateRemovalETL")
    SparkContext.getOrCreate(conf)
  }

  def runningSparkJob(createSparkContent: =>SparkContext, operator:SparkContext=>Unit, closeSparkContext:Boolean=false) = {
    val sc = createSparkContent
    try operator(sc)
    finally if(closeSparkContext) sc.stop()
  }
}

case class UserSetTopBoxInfo(
                          userId:String,
                          cityId:String,
                          createTime:String,
                          setTopBoxId:String,
                          productCode:String,
                          userType:Int,
                          setTopBoxType:Int
                          )

object UserSetTopBoxInfo {
  def parseLine(line:String) = {
    val infoItems = line split "\\|"
    new UserSetTopBoxInfo(infoItems(0), infoItems(1),infoItems(2),infoItems(3),infoItems(4),infoItems(5).toInt,infoItems(6).toInt)
  }
}



case class OrderInfo(
                      orderId:String,
                      username:String,
                      typeId:String,
                      originalPrice:String,
                      salePrice:String,
                      orderStatus:String,
                      createTime:Long,
                      modifyTime:Long,
                      operator:String,
                      untime:String
                    )

object OrderInfo {
  def parseLine(line:String) = {
    val infoItems = line split ","
    new OrderInfo(infoItems(0), infoItems(1),infoItems(2),infoItems(3),infoItems(4),infoItems(5),infoItems(6).toLong,infoItems(7).toLong,infoItems(8),infoItems(9))
  }
}

