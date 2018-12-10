package cn.goldlone.commerce.analysis

import cn.goldlone.commerce.dao.{DimensionDao, StatsOrderDao}
import cn.goldlone.commerce.etl.common.DateEnum
import cn.goldlone.commerce.etl.utils.TimeUtils
import cn.goldlone.commerce.model._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author Created by CN on 2018/12/8/0008 21:28 .
  */
object StatsOrderAnalysis {
  
  
  def main(args: Array[String]): Unit = {
  
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("order-analysis")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    
    val orderRequest = hiveContext.sql("select * from commerce.order_request")
    val orderSuccess = hiveContext.sql("select * from commerce.order_success")
    val orderRefund = hiveContext.sql("select * from commerce.order_refund")
  
    orderRequest.show()
    orderSuccess.show()
    orderRefund.show()
  
//    orderRequest.join(orderSuccess, Array("date", "platform", "currency_type", "payment_type"), "left_outer").show()
//
//    orderRequest.join(orderRefund, Array("date", "platform", "currency_type", "payment_type"), "left_outer").show()
    
    val df = orderRequest.join(orderSuccess, Array("date", "platform", "currency_type", "payment_type"), "left_outer")
        .join(orderRefund, Array("date", "platform", "currency_type", "payment_type"), "left_outer")
    
    df.show()
    
/*
+----------+--------+-------------+------------+-------------------------+--------------------------+-------------------------+--------------------------+------------------------+-------------------------+
|      date|platform|currency_type|payment_type|order_request_order_count|order_request_currency_sum|order_success_order_count|order_success_currency_sum|order_refund_order_count|order_refund_currency_sum|
+----------+--------+-------------+------------+-------------------------+--------------------------+-------------------------+--------------------------+------------------------+-------------------------+
|2017-07-04| website|            $|      alipay|                        1|                       300|                        2|                       600|                    null|                     null|
|2017-07-04| website|            $|      weixin|                        1|                       120|                     null|                      null|                       1|                      120|
|2017-07-04| website|          RMB|      alipay|                        2|                       650|                        2|                       650|                    null|                     null|
|2018-09-19| website|          RMB|      alipay|                        1|                       123|                        1|                       123|                    null|                     null|
|2018-09-19| website|          RMB|   weixinpay|                        1|                        54|                        1|                        54|                       1|                       54|
|2018-12-05| website|            $|      alipay|                        1|                       300|                     null|                      null|                    null|                     null|
+----------+--------+-------------+------------+-------------------------+--------------------------+-------------------------+--------------------------+------------------------+-------------------------+
*/
    df.foreachPartition(it => {
      val dimensionDao = new DimensionDao()
      val statsOrderDao = new StatsOrderDao()

      while(it.hasNext) {
        val row = it.next()

        val date = row.getString(0)
        val platform = row.getString(1)
        val currency_type = row.getString(2)
        val payment_type = row.getString(3)
        
        var order_request_count: Int = 0
        if(row.get(4) == null) {
          order_request_count = row.getLong(4).toInt
        }
        
        var order_request_currency_sum: Int = 0
        if(row.get(5) != null) {
          order_request_currency_sum = row.getLong(5).toInt
        }
        var order_success_order_count: Int = 0
        if(row.get(6) != null) {
          order_success_order_count = row.getLong(6).toInt
        }
        var order_success_currency_sum: Int = 0
        if(row.get(7) != null) {
          order_success_currency_sum = row.getLong(7).toInt
        }
        var order_refund_order_count: Int = 0
        if(row.get(8) != null) {
          order_refund_order_count = row.getLong(8).toInt
        }
        var order_refund_currency_sum: Int = 0
        if(row.get(9) != null) {
          order_refund_currency_sum = row.getLong(9).toInt
        }

        val ids = getDimensionIds((date, platform, currency_type, payment_type), dimensionDao)
        val history = statsOrderDao.getTotalRevenueAndRefundAmount(ids._2, ids._3, ids._4, ids._5)

        statsOrderDao.addOne(ids._1, ids._3, ids._4, ids._5, order_request_count,
          order_request_currency_sum, order_success_order_count, order_success_currency_sum,
          order_refund_order_count, order_refund_currency_sum, history._1, history._2)
      }

    })
    
    
  
    sc.stop()
  }
  
  // 获取维度相关信息
  def getDimensionIds(item: (String, String, String, String), dimensionDao: DimensionDao): (Int, Int, Int, Int, Int) = {
    val date = item._1 // 时间
    val platformName = item._2 // 平台名称
    val currencyTypeName = item._3 // 支付货币类型
    val paymentTypeName = item._4 // 支付方式
    
    // 时间维度id
    val timestamp = TimeUtils.parseString2Long(date, "yyyy-MM-dd")
    val dimensionDate = new DimensionDate(timestamp, DateEnum.DAY)// 设置为以天为单位的时间维度
    val dimensionDateId = dimensionDao.getDimensionIdByValue(dimensionDate)
    
    // 前一天的时间维度(1000*60*60*24 = 86400000)
    val yesterday = new DimensionDate(timestamp - 86400000, dimensionDate.dateType)
    val yesterdayId = dimensionDao.getDimensionIdByValue(yesterday)
  
    // 平台维度id
    val platform = new DimensionPlatform(platformName)
    val dimensionPlatformId = dimensionDao.getDimensionIdByValue(platform)
    
    // 货币类型维度id
    val dimensionCurrencyType = new DimensionCurrencyType(currencyTypeName)
    val dimensionCurrencyTypeId = dimensionDao.getDimensionIdByValue(dimensionCurrencyType)
    
    // 支付方式类型维度
    val dimensionPaymentType = new DimensionPaymentType(paymentTypeName)
    val dimensionPaymentTypeId = dimensionDao.getDimensionIdByValue(dimensionPaymentType)
    
    
    (dimensionDateId, yesterdayId, dimensionPlatformId, dimensionCurrencyTypeId, dimensionPaymentTypeId)
  }
  
  
  
}
