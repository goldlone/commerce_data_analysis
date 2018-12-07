package cn.goldlone.commerce.etl.spark

import cn.goldlone.commerce.dao.{DimensionDao, StatsDeviceLocationDao}
import cn.goldlone.commerce.etl.common.{DateEnum, EventEnum, EventLogConstants}
import cn.goldlone.commerce.etl.utils.{LogUtil, TimeUtils}
import cn.goldlone.commerce.model.{DimensionDate, DimensionLocation, DimensionPlatform}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 地域信息信息模块
  * 统计活跃用户、Session数量和跳出会话数
  *
  * @author Created by CN on 2018/12/7/0007 14:26 .
  */
object LocationAnalysis {
  
  
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("location_analysis")
    val sc = new SparkContext(sparkConf)
    
    // 读取日志信息
    val logRdd = sc.textFile("hdfs://hh:9000/data/commerce/nginx/2018/12/05")
    
    // 数据操作相关类
    val dimensionDao = new DimensionDao()
    val statsDeviceLocationDao = new StatsDeviceLocationDao()
  
    // 解析日志信息
    val accessRdd: RDD[mutable.HashMap[String, String]] = logRdd.map(LogUtil.handleLog).filter(_ != null).cache()
  
    // 仅包含pageView事件
    val pageViewEventRdd = accessRdd.filter(map => {
      EventEnum.PAGE_VIEW.getAlias.equals(map(EventLogConstants.LOG_EVENT))
    }).cache()
  
    
    /** 1. 活跃用户 */
    pageViewEventRdd.mapPartitions(it => {
      var map = new mutable.HashMap[String, (String,String,String,String,String)]()
    
      // 每个分区中过滤掉相同的UUID，减少分组时shuffle的数据量
      while(it.hasNext) {
        val item = it.next()
        // (uuid, (时间戳, 平台, 国家, 省份, 城市))
        val uuid = item(EventLogConstants.LOG_UUID)
      
        if(!map.contains(uuid)) {
          map += (uuid -> (item(EventLogConstants.LOG_SERVER_TIME), item(EventLogConstants.LOG_PLATFORM),
              item(EventLogConstants.LOG_REGION_COUNTRY), item(EventLogConstants.LOG_REGION_PROVINCE),
              item(EventLogConstants.LOG_REGION_CITY)))
        }
      }
    
      map.iterator
    }).groupByKey().map(item => { // 根据UUID去重，调整数据格式，准备分组统计
      // 调整 K, V 顺序， 统计时间, 平台维度, 国家, 省份, 城市
      val values = item._2.head // 选第一个
      val time = TimeUtils.parseLong2String(values._1.toLong)
      ((time, values._2, values._3, values._4, values._5), 1)
    }).reduceByKey(_ + _).collect().foreach(item => {
      // 计算相关维度
      val ids = getDimensionIds(item._1)
    
      // 时间维度id
      val dimensionDateId = ids._1
      // 平台维度id
      val dimensionPlatformId = ids._2
      // 浏览器维度id
      val dimensionLocationId = ids._3
    
      // 活跃用户总数
      val activeUserCount = item._2
      println(s"【活跃用户统计】 => 日期: $dimensionDateId, 平台: $dimensionPlatformId, 地区: $dimensionLocationId, 活跃用户总数: $activeUserCount")

      // 将统计数据写入数据库
      statsDeviceLocationDao.addActiveUserCount(dimensionDateId, dimensionPlatformId, dimensionLocationId, activeUserCount)
    })
    
    
    /** 2. Session数量*/
    pageViewEventRdd.mapPartitions(it => {
      var map = new mutable.HashMap[String, (String,String,String,String,String)]()
    
      // 每个分区中过滤掉相同的UUID，减少分组时shuffle的数据量
      while(it.hasNext) {
        val item = it.next()
        // (sessionId, (时间戳, 平台, 国家, 省份, 城市))
        val sessionId = item(EventLogConstants.LOG_SESSION_ID)
      
        if(!map.contains(sessionId)) {
          map += (sessionId -> (item(EventLogConstants.LOG_SERVER_TIME), item(EventLogConstants.LOG_PLATFORM),
              item(EventLogConstants.LOG_REGION_COUNTRY), item(EventLogConstants.LOG_REGION_PROVINCE),
              item(EventLogConstants.LOG_REGION_CITY)))
        }
      }
    
      map.iterator
    }).groupByKey().map(item => { // 根据SessionId去重，调整数据格式，准备分组统计
      // 调整 K, V 顺序， 统计时间, 平台维度, 国家, 省份, 城市
      val values = item._2.head // 选第一个
    val time = TimeUtils.parseLong2String(values._1.toLong)
      ((time, values._2, values._3, values._4, values._5), 1)
    }).reduceByKey(_ + _).collect().foreach(item => {
      // 计算相关维度
      val ids = getDimensionIds(item._1)
    
      // 时间维度id
      val dimensionDateId = ids._1
      // 平台维度id
      val dimensionPlatformId = ids._2
      // 浏览器维度id
      val dimensionLocationId = ids._3
    
      // 活跃用户总数
      val sessionCount = item._2
      println(s"【会话个数统计】 => 日期: $dimensionDateId, 平台: $dimensionPlatformId, 地区: $dimensionLocationId, 会话总数: $sessionCount")
    
      // 将统计数据写入数据库
      statsDeviceLocationDao.addSessionCount(dimensionDateId, dimensionPlatformId, dimensionLocationId, sessionCount)
    })
    
    
    /** 3. 跳出会话数：PV数为1的Session总数 */
    pageViewEventRdd.mapPartitions(it => {
      it.map(item => {
        val sessionId = item(EventLogConstants.LOG_SESSION_ID)
        val serverTime = item(EventLogConstants.LOG_SERVER_TIME)
        val platform = item(EventLogConstants.LOG_PLATFORM)
        val country = item(EventLogConstants.LOG_REGION_COUNTRY)
        val province = item(EventLogConstants.LOG_REGION_PROVINCE)
        val city = item(EventLogConstants.LOG_REGION_CITY)
        
        (sessionId, (1, serverTime, platform, country, province, city))
      })
    }).reduceByKey((x, y) => (x._1 + y._1, x._2, x._3, x._4, x._5, x._6))
        .filter(_._2._1 == 1).map(item => {
      (item._2, 1)
    }).reduceByKey(_ + _).collect().foreach(item => {
      val date = item._1._2 // 时间
      val platformName = item._1._3 // 平台名称
      val country = item._1._4 // 国家
      val province = item._1._5 // 省份
      val city = item._1._6 // 城市
      
      val bounceSessionsCount = item._2 // 跳出会话个数
  
      // 计算相关维度
      val ids = getDimensionIds((date, platformName, country, province, city))
  
      // 时间维度id
      val dimensionDateId = ids._1
      // 平台维度id
      val dimensionPlatformId = ids._2
      // 浏览器维度id
      val dimensionLocationId = ids._3
  
      
      println(s"【跳出会话个数统计】 => 日期: $dimensionDateId, 平台: $dimensionPlatformId, 地区: $dimensionLocationId, 跳出会话个数: $bounceSessionsCount")
  
      // 将统计数据写入数据库
      statsDeviceLocationDao.addBounceSessionCount(dimensionDateId, dimensionPlatformId, dimensionLocationId, bounceSessionsCount)
    })
    
    
  
    // 获取维度相关信息
    def getDimensionIds(item: (String, String, String, String, String)): (Int, Int, Int) = {
      val date = item._1 // 时间
      val platformName = item._2 // 平台名称
      val country = item._3 // 国家
      val province = item._4 // 省份
      val city = item._5 // 城市
    
      // 时间维度id
      val timestamp = TimeUtils.parseString2Long(date, "yyyy-MM-dd")
      val dimensionDate = new DimensionDate(timestamp, DateEnum.DAY)// 设置为以天为单位的时间维度
      val dimensionDateId = dimensionDao.getDimensionIdByValue(dimensionDate)
  
      // 平台维度id
      val platform = new DimensionPlatform(platformName)
      val dimensionPlatformId = dimensionDao.getDimensionIdByValue(platform)
    
      // 地区维度id
      val location = new DimensionLocation(country, province, city)
      val dimensionLocationId = dimensionDao.getDimensionIdByValue(location)
    
    
      (dimensionDateId, dimensionPlatformId, dimensionLocationId)
    }
  
    sc.stop()
  }


}
