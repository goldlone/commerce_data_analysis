package cn.goldlone.commerce.analysis

import cn.goldlone.commerce.dao.{DimensionDao, StatsDeviceBrowserDao, StatsUserDao}
import cn.goldlone.commerce.etl.common.{DateEnum, EventEnum, EventLogConstants}
import cn.goldlone.commerce.etl.utils.{LogUtil, TimeUtils}
import cn.goldlone.commerce.model.{DimensionBrowser, DimensionDate, DimensionPlatform}
import cn.goldlone.commerce.utils.DBUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 分析nginx日志
  * @author Created by CN on 2018/12/4/0004 12:33 .
  */
object UserAnalysisDay {
  
  def main(args: Array[String]): Unit = {
  
    val sparkConf = new SparkConf().setAppName("nginx-analysis").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    
//    val conf = new Configuration()
//    conf.set("fs.defaultFS", "hdfs://hh:9000")
//    val fs = FileSystem.get(conf)
//    val fileStatuses = fs.listStatus(new Path("/data/commerce/nginx/2018/12"))
//    for(status <- fileStatuses) {
//      val path = status.getPath.toString
//      // 循环遍历
//    }
    
    val dates = Array("05", "06", "07")
    
    for(day <- dates) {
    
      // 读取日志信息
      val logRdd = sc.textFile("hdfs://hh:9000/data/commerce/nginx/2018/12/" + day)
    
    
      // 数据操作相关类
      val dimensionDao = new DimensionDao()
      val statsDeviceBrowserDao = new StatsDeviceBrowserDao()
      val statsUserDao = new StatsUserDao()
      
      
      // 解析日志信息
      val accessRdd: RDD[mutable.HashMap[String, String]] = logRdd.map(LogUtil.handleLog).filter(_ != null).cache()
      
      // 仅包含launch事件
      val launchEventRdd = accessRdd.filter(map => { // 过滤留下launch事件
        EventEnum.LAUNCH.getAlias.equals(map(EventLogConstants.LOG_EVENT))
      }).cache()
      
      // 仅包含pageView事件
      val pageViewEventRdd = accessRdd.filter(map => {
        EventEnum.PAGE_VIEW.getAlias.equals(map(EventLogConstants.LOG_EVENT))
      }).cache()
      
      
      
      
      // 1. 活跃用户数量               pageView   uuid去重总数
      // 2. 新增用户及总用户数量        launch      uuid去重总数
      // 3. 活跃会员数量               pageView   memberId去重总数
      // 4. 新增会员数量及总会员数量     launch     memberId去重总数
      // 5. session数量               所有事件    session去重总数
      // 6. session总长度             所有事件    同一Session的最大时差
      // 7. pv统计                    pageView   求和
      
      
      /** 1. 统计活跃用户 */
      // *活跃用户总数*：pageView事件中uuid去重后的个数，如果用户没有pv，就不算活跃（系统触发的charge_事件不算活跃）
      pageViewEventRdd.mapPartitions(it => {
        var map = new mutable.HashMap[String, (String,String,String,String)]()
  
        // 每个分区中过滤掉相同的UUID，减少分组时shuffle的数据量
        while(it.hasNext) {
          val item = it.next()
          // (uuid, (时间戳, 浏览器名称, 浏览器版本, 平台))
          val uuid = item(EventLogConstants.LOG_UUID)
  
          if(!map.contains(uuid)) {
            map += (uuid -> (item(EventLogConstants.LOG_SERVER_TIME), item(EventLogConstants.LOG_BROWSER_NAME),
                item(EventLogConstants.LOG_BROWSER_VERSION), item(EventLogConstants.LOG_PLATFORM)))
          }
        }
  
        map.iterator
      }).groupByKey().map(item => { // 根据UUID去重，调整数据格式，准备分组统计
        // 调整 K, V 顺序， 统计时间、浏览器、平台维度的数据
        val values = item._2.head // 选第一个
        val time = TimeUtils.parseLong2String(values._1.toLong)
        ((time, values._2, values._3, values._4), 1)
      }).reduceByKey(_ + _).collect().foreach(item => {
        // 计算相关维度
        val ids = getDimensionIds1(item._1)
  
        // 时间维度id
        val dimensionDateId = ids._1
  
        // 浏览器维度id
        val dimensionBrowserId = ids._3
  
        // 平台维度id
        val dimensionPlatformId = ids._4
  
        // 活跃用户总数
        val activeUserCount = item._2
        println(s"【活跃用户统计】 => 日期: $dimensionDateId, 浏览器: $dimensionBrowserId, 平台: $dimensionPlatformId, 活跃用户总数: $activeUserCount")
  
        // 将统计数据写入数据库
        // 1. 时间-浏览器-平台 维度
        statsDeviceBrowserDao.addActiveUserCount(dimensionDateId, dimensionBrowserId, dimensionPlatformId, activeUserCount)
        // 2. 时间-平台 维度 (通过update累加可以实现目的)
        statsUserDao.addActiveUserCount(dimensionDateId, dimensionPlatformId, activeUserCount)
      })
    
      /** 3. 统计活跃会员数 */
      accessRdd.filter(item => { // 过滤 保留具有会员号和pageView事件的记录
        item.getOrElse(EventLogConstants.LOG_MEMBER_ID, null) != null
      }).mapPartitions(it => {
        var map = new mutable.HashMap[String, (String,String,String,String)]()
  
        // 每个分区中过滤掉相同的memberId，减少分组时shuffle的数据量
        while(it.hasNext) {
          val item = it.next()
          // (memberId, (时间戳, 浏览器名称, 浏览器版本, 平台))
          val memberId = item(EventLogConstants.LOG_MEMBER_ID)
  
          if(!map.contains(memberId)) {
            map += (memberId -> (item(EventLogConstants.LOG_SERVER_TIME), item(EventLogConstants.LOG_BROWSER_NAME),
                item(EventLogConstants.LOG_BROWSER_VERSION), item(EventLogConstants.LOG_PLATFORM)))
          }
        }
  
        map.iterator
      }).groupByKey().map(item => {
        val values = item._2.head // 选第一个
        val time = TimeUtils.parseLong2String(values._1.toLong)
        ((time, values._2, values._3, values._4), 1)
      }).reduceByKey(_ + _).collect().foreach(item => {
        // 计算相关维度
        val ids = getDimensionIds1(item._1)
        // 时间维度id
        val dimensionDateId = ids._1
        // 浏览器维度id
        val dimensionBrowserId = ids._3
        // 平台维度id
        val dimensionPlatformId = ids._4
        // 活跃会员总数
        val activeMemberCount = item._2
        println(s"【活跃会员统计】 => 日期: $dimensionDateId, 浏览器: $dimensionBrowserId, 平台: $dimensionPlatformId, 活跃会员总数: $activeMemberCount")
  
        // 将统计数据写入数据库
        // 1. 时间-浏览器-平台 维度
        statsDeviceBrowserDao.addActiveMemberCount(dimensionDateId, dimensionBrowserId, dimensionPlatformId, activeMemberCount)
        // 2. 时间-平台 维度 (通过update累加可以实现目的)
        statsUserDao.addActiveMemberCount(dimensionDateId, dimensionPlatformId, activeMemberCount)
      })
    
      
      
      /** 2. 统计新增用户、总用户 */
      // *新增用户数*：launch事件的总数就代表着新增的用户数量
      launchEventRdd.map(item => {
        val time = TimeUtils.parseLong2String(item(EventLogConstants.LOG_SERVER_TIME).toLong)
        val browserName = item(EventLogConstants.LOG_BROWSER_NAME)
        val browserVersion = item(EventLogConstants.LOG_BROWSER_VERSION)
        val platform = item(EventLogConstants.LOG_PLATFORM)
        val uuid = item(EventLogConstants.LOG_UUID)
      
        // ((时间_天, 浏览器名称, 浏览器版本, 平台), UUID)
        ((time, browserName, browserVersion, platform), uuid)
      }).groupByKey().collect().foreach(item => {
        // 计算相关维度
        val ids = getDimensionIds1(item._1)
        // 时间维度id
        val dimensionDateId = ids._1
      
        // 前一天的时间纬度
        val yesterdayId = ids._2
      
        // 浏览器维度id
        val dimensionBrowserId = ids._3
      
        // 平台维度id
        val dimensionPlatformId = ids._4
      
        // 新增用户数量
        val newInstallUserCount = item._2.toSet.size
        
        // 计算总用户数
        var totalInstallUserCount = statsDeviceBrowserDao.getTotalInstallUserCount(yesterdayId, dimensionBrowserId, dimensionPlatformId)
        totalInstallUserCount += newInstallUserCount
    
        println(s"【总用户统计】 => 日期: $dimensionDateId, 浏览器: $dimensionBrowserId, 平台: $dimensionPlatformId, 总用户数: $totalInstallUserCount")
        println(s"【新增用户统计】 => 日期: $dimensionDateId, 浏览器: $dimensionBrowserId, 平台: $dimensionPlatformId, 新增用户数: $newInstallUserCount")
      
        // 将统计数据写入数据库
        // 1. 时间-浏览器-平台 维度
        statsDeviceBrowserDao.addNewInstallUserCount(dimensionDateId, dimensionBrowserId,
          dimensionPlatformId, totalInstallUserCount, newInstallUserCount)
      })
      // 向用户基本信息表里统计
      launchEventRdd.filter(item => { // 过滤 保留具有会员号和pageView事件的记录
        item.getOrElse(EventLogConstants.LOG_UUID, null) != null
      }).map(item => {
        val time = TimeUtils.parseLong2String(item(EventLogConstants.LOG_SERVER_TIME).toLong)
        val platform = item(EventLogConstants.LOG_PLATFORM)
        val uuid = item(EventLogConstants.LOG_UUID)
    
        // ((时间_天, 平台), uuid)
        ((time, platform), uuid)
      }).groupByKey().collect().foreach(item => {
        // 计算相关维度
        val ids = getDimensionIds2(item._1)
        // 时间维度id
        val dimensionDateId = ids._1
    
        // 前一天的时间纬度
        val yesterdayId = ids._2
    
        // 平台维度id
        val dimensionPlatformId = ids._3
    
        // 新增用户数量
        val newInstallUserCount = item._2.toSet.size
    
        // 计算总会员数
        var totalInstallUserCount = statsUserDao.getTotalInstallUserCount(yesterdayId, dimensionPlatformId)
        totalInstallUserCount += newInstallUserCount
    
        println(s"【用户-总用户统计】 => 日期: $dimensionDateId, 平台: $dimensionPlatformId, 新增会员数: $newInstallUserCount")
        println(s"【用户-新增用户统计】 => 日期: $dimensionDateId, 平台: $dimensionPlatformId, 新增会员数: $newInstallUserCount")
    
        // 2. 时间-平台 维度
        statsUserDao.addNewInstallUserCount(dimensionDateId, dimensionPlatformId,
          totalInstallUserCount, newInstallUserCount)
      })
      
      /** 4. 统计新增会员、总会员 */
      // *新增用户数*：launch事件的总数就代表着新增的用户数量
      launchEventRdd.filter(item => { // 过滤 保留具有会员号和pageView事件的记录
        item.getOrElse(EventLogConstants.LOG_MEMBER_ID, null) != null
      }).map(item => {
        val time = TimeUtils.parseLong2String(item(EventLogConstants.LOG_SERVER_TIME).toLong)
        val browserName = item(EventLogConstants.LOG_BROWSER_NAME)
        val browserVersion = item(EventLogConstants.LOG_BROWSER_VERSION)
        val platform = item(EventLogConstants.LOG_PLATFORM)
        val memberId = item(EventLogConstants.LOG_MEMBER_ID)
      
        // ((时间_天, 浏览器名称, 浏览器版本, 平台), memberId)
        ((time, browserName, browserVersion, platform), memberId)
      }).groupByKey().collect().foreach(item => {
        // 计算相关维度
        val ids = getDimensionIds1(item._1)
        // 时间维度id
        val dimensionDateId = ids._1
      
        // 前一天的时间纬度
        val yesterdayId = ids._2
      
        // 浏览器维度id
        val dimensionBrowserId = ids._3
      
        // 平台维度id
        val dimensionPlatformId = ids._4
    
        // 新增用户数量
        val newMemberCount = item._2.toSet.size
        
        // 计算总会员数
        var totalMemberCount = statsDeviceBrowserDao.getTotalMemberCount(yesterdayId, dimensionBrowserId, dimensionPlatformId)
        totalMemberCount += newMemberCount
        
        println(s"【总会员统计】 => 日期: $dimensionDateId, 浏览器: $dimensionBrowserId, 平台: $dimensionPlatformId, 新增会员数: $totalMemberCount")
        println(s"【新增会员统计】 => 日期: $dimensionDateId, 浏览器: $dimensionBrowserId, 平台: $dimensionPlatformId, 新增会员数: $newMemberCount")
      
        // 将统计数据写入数据库
        // 1. 时间-浏览器-平台 维度
        statsDeviceBrowserDao.addNewMemberUserCount(dimensionDateId, dimensionBrowserId,
          dimensionPlatformId, totalMemberCount, newMemberCount)
      })
      // 向用户基本信息表里统计
      launchEventRdd.filter(item => { // 过滤 保留具有会员号和pageView事件的记录
        item.getOrElse(EventLogConstants.LOG_MEMBER_ID, null) != null
      }).map(item => {
        val time = TimeUtils.parseLong2String(item(EventLogConstants.LOG_SERVER_TIME).toLong)
        val platform = item(EventLogConstants.LOG_PLATFORM)
        val memberId = item(EventLogConstants.LOG_MEMBER_ID)
    
        // ((时间_天, 平台), memberId)
        ((time, platform), memberId)
      }).groupByKey().collect().foreach(item => {
        // 计算相关维度
        val ids = getDimensionIds2(item._1)
        // 时间维度id
        val dimensionDateId = ids._1
    
        // 前一天的时间纬度
        val yesterdayId = ids._2
    
        // 平台维度id
        val dimensionPlatformId = ids._3
    
        // 新增用户数量
        val newMemberCount = item._2.toSet.size
    
        // 计算总会员数
        var totalMemberCount = statsUserDao.getTotalMemberCount(yesterdayId, dimensionPlatformId)
        totalMemberCount += newMemberCount
        
        println(s"【用户-总会员统计】 => 日期: $dimensionDateId, 平台: $dimensionPlatformId, 新增会员数: $totalMemberCount")
        println(s"【用户-新增会员统计】 => 日期: $dimensionDateId, 平台: $dimensionPlatformId, 新增会员数: $newMemberCount")
  
        // 2. 时间-平台 维度
        statsUserDao.addNewMemberUserCount(dimensionDateId, dimensionPlatformId,
          totalMemberCount, newMemberCount)
      })
    
    
      /** 5. 统计Session数量和时长 */
      accessRdd.map(item => {
        val sessionId = item(EventLogConstants.LOG_SESSION_ID)
        
        (sessionId, (item(EventLogConstants.LOG_SERVER_TIME), item(EventLogConstants.LOG_BROWSER_NAME),
                      item(EventLogConstants.LOG_BROWSER_VERSION), item(EventLogConstants.LOG_PLATFORM)))
      }).groupByKey().map(item => { // 通过groupby分组shuffle太多了，想想怎么优化
  //      val sessionId = item._1
        val values = item._2
        
        val min = values.minBy(x => x._1)
        val max = values.maxBy(x => x._1)
        
        val len = max._1.toLong - min._1.toLong
        
        val head = values.head
        val time = TimeUtils.parseLong2String(head._1.toLong)
        ((time, head._2, head._3, head._4), (1, len))
      }).reduceByKey((x, y) => (x._1+y._1, x._2+y._2)).collect().foreach(item => {
        val ids = getDimensionIds1(item._1)
    
        // 时间维度id
        val dimensionDateId = ids._1
        // 浏览器维度id
        val dimensionBrowserId = ids._3
        // 平台维度id
        val dimensionPlatformId = ids._4
        
        val sessionCount = item._2._1
        val sessionLength = item._2._2.toInt
        
        println(s"【Session统计】 => 日期: $dimensionDateId, 浏览器: $dimensionBrowserId, 平台: $dimensionPlatformId, Session总数: $sessionCount, Session总时长: $sessionLength")
        
        // 将统计结果写入数据库
        // 1. 时间-浏览器-平台 维度
        statsDeviceBrowserDao.addSessionCount(dimensionDateId, dimensionBrowserId,
          dimensionPlatformId, sessionCount, sessionLength)
        // 2. 时间-平台 维度
        statsUserDao.addSessionCount(dimensionDateId, dimensionPlatformId,
          sessionCount, sessionLength)
      })
      
      
      /** 7. PageView事件统计 */
      pageViewEventRdd.map(item => {
        val time = TimeUtils.parseLong2String(item(EventLogConstants.LOG_SERVER_TIME).toLong)
        val browserName = item(EventLogConstants.LOG_BROWSER_NAME)
        val browserVersion = item(EventLogConstants.LOG_BROWSER_VERSION)
        val platform = item(EventLogConstants.LOG_PLATFORM)
    
        // ((时间_天, 浏览器名称, 浏览器版本, 平台), 1)
        ((time, browserName, browserVersion, platform), 1)
      }).reduceByKey(_ + _).collect().foreach(item => {
        val ids = getDimensionIds1(item._1)
    
        // 时间维度id
        val dimensionDateId = ids._1
        // 浏览器维度id
        val dimensionBrowserId = ids._3
        // 平台维度id
        val dimensionPlatformId = ids._4
    
        // pageView事件数
        val pv = item._2
        
        println(s"【PV统计】 => 日期: $dimensionDateId, 浏览器: $dimensionBrowserId, 平台: $dimensionPlatformId, PV总数: $pv")
    
        // 将统计结果写入数据库
        // 时间-浏览器-平台 维度
        statsDeviceBrowserDao.addPageViewCount(dimensionDateId, dimensionBrowserId,
          dimensionPlatformId, pv)
      })
      
      
    
      // 获取维度相关信息
      def getDimensionIds1(item: (String, String, String, String)): (Int, Int, Int, Int) = {
        val date = item._1 // 时间
        val browserName = item._2 // 浏览器名称
        val browserVersion = item._3 // 浏览器版本
        val platformName = item._4 // 平台名称
  
        val timestamp = TimeUtils.parseString2Long(date, "yyyy-MM-dd")
        // 前一天的时间维度(1000*60*60*24 = 86400000)
        val yesterday = new DimensionDate(timestamp - 86400000, DateEnum.DAY)
        val yesterdayId = dimensionDao.getDimensionIdByValue(yesterday)
        
        // 时间维度id
        val dimensionDate = new DimensionDate(timestamp, DateEnum.DAY)// 设置为以天为单位的时间维度
        val dimensionDateId = dimensionDao.getDimensionIdByValue(dimensionDate)
        
        // 浏览器维度id
        val browser = new DimensionBrowser(browserName, browserVersion)
        val dimensionBrowserId = dimensionDao.getDimensionIdByValue(browser)
      
        // 平台维度id
        val platform = new DimensionPlatform(platformName)
        val dimensionPlatformId = dimensionDao.getDimensionIdByValue(platform)
      
        (dimensionDateId, yesterdayId, dimensionBrowserId, dimensionPlatformId)
      }
      
      // 获取维度相关信息
      def getDimensionIds2(item: (String, String)): (Int, Int, Int) = {
        val date = item._1 // 时间
        val platformName = item._2 // 平台名称
  
        val timestamp = TimeUtils.parseString2Long(date, "yyyy-MM-dd")
        // 前一天的时间维度(1000*60*60*24 = 86400000)
        val yesterday = new DimensionDate(timestamp - 86400000, DateEnum.DAY)
        val yesterdayId = dimensionDao.getDimensionIdByValue(yesterday)
        
        // 时间维度id
        val dimensionDate = new DimensionDate(timestamp, DateEnum.DAY)// 设置为以天为单位的时间维度
        val dimensionDateId = dimensionDao.getDimensionIdByValue(dimensionDate)
      
        // 平台维度id
        val platform = new DimensionPlatform(platformName)
        val dimensionPlatformId = dimensionDao.getDimensionIdByValue(platform)
      
        (dimensionDateId, yesterdayId, dimensionPlatformId)
      }
  
    }
    
    DBUtil.releaseResource()
    sc.stop()
  }
  
}

