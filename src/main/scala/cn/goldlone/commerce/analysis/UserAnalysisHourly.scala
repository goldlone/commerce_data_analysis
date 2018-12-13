package cn.goldlone.commerce.analysis

import cn.goldlone.commerce.dao.{DimensionDao, StatsHourlyDao}
import cn.goldlone.commerce.etl.common.{DateEnum, EventEnum, EventLogConstants, KpiEnum}
import cn.goldlone.commerce.etl.utils.{LogUtil, TimeUtils}
import cn.goldlone.commerce.model.{DimensionDate, DimensionKpi, DimensionPlatform}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 以小时为间隔分析数据
  *
  * @author Created by CN on 2018/12/6/0006 21:10 .
  */
object UserAnalysisHourly {
  
  def main(args: Array[String]): Unit = {
  
    val sparkConf = new SparkConf().setAppName("nginx-analysis").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    
    // 1. 统计每小时的 活跃用户
    // 2. 统计每小时的 会话个数
    // 3. 统计每小时的 会话长度
  
    val dates = Array("05", "06", "07")
  
    for(day <- dates) {
  
      // 读取日志信息
      val logRdd = sc.textFile("hdfs://hh:9000/data/commerce/nginx/2018/12/" + day)
  
  
      // 数据操作相关类
      val dimensionDao = new DimensionDao()
      val statsHourlyDao = new StatsHourlyDao()
  
      // 解析日志信息
      val accessRdd: RDD[mutable.HashMap[String, String]] = logRdd.map(LogUtil.handleLog).filter(_ != null).cache()
      // 仅包含pageView事件
      val pageViewEventRdd = accessRdd.filter(map => {
        EventEnum.PAGE_VIEW.getAlias.equals(map(EventLogConstants.LOG_EVENT))
      }).cache()
  
      /** 1. 统计活跃用户 */
      // *活跃用户总数*：pageView事件中uuid去重后的个数，如果用户没有pv，就不算活跃（系统触发的charge_事件不算活跃）
      pageViewEventRdd.mapPartitions(it => {
        var map = new mutable.HashMap[String, (String, String)]()
    
        // 每个分区中过滤掉相同的UUID，减少分组时shuffle的数据量
        while (it.hasNext) {
          val item = it.next()
          // (uuid, (时间戳, 平台))
          val uuid = item(EventLogConstants.LOG_UUID)
      
          if (!map.contains(uuid)) {
            map += (uuid -> (item(EventLogConstants.LOG_SERVER_TIME), item(EventLogConstants.LOG_PLATFORM)))
          }
        }
    
        map.iterator
      }).groupByKey().flatMap(item => { // 根据UUID去重，调整数据格式，准备分组统计
        // 调整 K, V 顺序， 统计时间、平台维度的数据
        var list = new ListBuffer[((String, Int, String), Int)]
    
        item._2.foreach(item => {
          val timestamp = item._1.toLong
          val date = TimeUtils.parseLong2String(timestamp)
          val hour = TimeUtils.getDateInfo(timestamp, DateEnum.HOUR)
      
          // 日期, 小时, 平台
          list += (((date, hour, item._2), 1))
        })
    
        list
      }).reduceByKey(_ + _).collect().map(item => {
        ((item._1._1, item._1._3), (item._1._2, item._2))
      }).groupBy(x => x._1).map(x => {
        // (日期, 平台), (小时, count)
        (x._1, x._2.map(y => y._2).toMap)
      }).foreach(item => {
    
        addKpi(item, KpiEnum.ACTIVITY_USER)
    
        // 计算相关维度
        val ids = getDimensionIds(item._1._1, item._1._2, KpiEnum.ACTIVITY_USER.getName)
    
        // 时间维度id
        val dimensionDateId = ids._1
        // 平台维度id
        val dimensionPlatformId = ids._2
        // Kpi维度id
        val dimensionKpiId = ids._3
    
        // 将统计数据写入数据库
        statsHourlyDao.addKpiCount(dimensionDateId, dimensionPlatformId, dimensionKpiId, item._2)
      })
  
  
      /** 2. 统计Session数量和时长 */
      accessRdd.map(item => {
        val sessionId = item(EventLogConstants.LOG_SESSION_ID)
    
        (sessionId, (item(EventLogConstants.LOG_SERVER_TIME), item(EventLogConstants.LOG_PLATFORM)))
      }).groupByKey().flatMap(item => { // 通过groupby分组shuffle太多了，想想怎么优化
        val list = new ListBuffer[((String, Int, String), (Int, Int))]
    
        val values = item._2
    
        val min = values.minBy(x => x._1)
        val max = values.maxBy(x => x._1)
        // session时长
        val len = max._1.toLong - min._1.toLong
    
        item._2.foreach(item => {
          val timestamp = item._1.toLong
          val date = TimeUtils.parseLong2String(timestamp)
          val hour = TimeUtils.getDateInfo(timestamp, DateEnum.HOUR)
      
          // 日期, 小时, 平台
          list += (((date, hour, item._2), (1, len.toInt)))
        })
    
        list
      }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).collect().map(item => {
        // (日期, 平台), (小时 , count)
        ((item._1._1, item._1._3), (item._1._2, item._2))
      }).groupBy(x => x._1).map(x => {
        // (日期, 平台), (小时 -> count)
        (x._1, x._2.map(y => y._2).toMap)
      }).foreach(item => {
    
        val count = item._2.map(x => (x._1, x._2._1))
        val lens = item._2.map(x => (x._1, x._2._2))
    
        addKpi((item._1, count), KpiEnum.SESSION_COUNT)
        addKpi((item._1, lens), KpiEnum.SESSION_LENGTH)
      })
  
  
      /**
        * 获取维度相关信息
        *
        * @param item
        * @return
        */
      def getDimensionIds(item: (String, String, String)): (Int, Int, Int) = {
        val date = item._1 // 时间
        val platformName = item._2 // 平台名称
        val kpiName = item._3 // Kpi名称
    
        // 时间维度id
        val timestamp = TimeUtils.parseString2Long(date, "yyyy-MM-dd")
        val dimensionDate = new DimensionDate(timestamp, DateEnum.DAY)
        // 设置为以天为单位的时间维度
        val dimensionDateId = dimensionDao.getDimensionIdByValue(dimensionDate)
    
        // kpi维度id
        val kpi = new DimensionKpi(kpiName)
        val dimensionKpiId = dimensionDao.getDimensionIdByValue(kpi)
    
        // 平台维度id
        val platform = new DimensionPlatform(platformName)
        val dimensionPlatformId = dimensionDao.getDimensionIdByValue(platform)
    
        (dimensionDateId, dimensionPlatformId, dimensionKpiId)
      }
  
  
      /**
        * 添加KPI指标
        *
        * @param item
        * @param kpiType
        */
      def addKpi(item: ((String, String), Map[Int, Int]), kpiType: KpiEnum): Unit = {
        // 计算相关维度
        val ids = getDimensionIds(item._1._1, item._1._2, kpiType.getName)
    
        // 时间维度id
        val dimensionDateId = ids._1
        // 平台维度id
        val dimensionPlatformId = ids._2
        // Kpi维度id
        val dimensionKpiId = ids._3
    
        // 将统计数据写入数据库
        statsHourlyDao.addKpiCount(dimensionDateId, dimensionPlatformId, dimensionKpiId, item._2)
      }
    }
  
    sc.stop()
  }


  
}
