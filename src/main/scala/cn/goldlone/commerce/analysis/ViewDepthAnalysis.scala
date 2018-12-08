package cn.goldlone.commerce.analysis

import cn.goldlone.commerce.dao.{DimensionDao, StatsViewDepthDao}
import cn.goldlone.commerce.etl.common.{DateEnum, KpiEnum}
import cn.goldlone.commerce.etl.utils.TimeUtils
import cn.goldlone.commerce.model.{DimensionDate, DimensionKpi, DimensionPlatform}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 用户浏览深度分析
  * 通过pv值来表示用户的浏览深度，结合用户和会话两种维度来分析数据
 *
  * @author Created by CN on 2018/12/7/0007 23:39 .
  */
object ViewDepthAnalysis {
  
  
  def main(args: Array[String]): Unit = {
  
    val conf = new SparkConf().setMaster("local[*]").setAppName("hive-test")
    val sc = new SparkContext(conf)
  
    val hiveContext = new HiveContext(sc)
    
    // 1.1 根据时间，平台，用户 分组，统计pageView的个数
    hiveContext.sql("drop table if exists commerce.stats_view_depth_time_platform_user")
    hiveContext.sql("create table commerce.stats_view_depth_time_platform_user " +
        "as " +
        "select from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd') date, " +
        "    platform, " +
        "    uuid, " +
        "    count(1) pv, " +
        "    case when count(1) <= 0 then 'other' " +
        "      when count(1) = 1 then 'pv1' " +
        "      when count(1) = 2 then 'pv2' " +
        "      when count(1) = 3 then 'pv3' " +
        "      when count(1) = 4 then 'pv4' " +
        "      when count(1) <= 10 then 'pv5_10' " +
        "      when count(1) <= 30 then 'pv10_30' " +
        "      when count(1) <= 60 then 'pv30_60' " +
        "      when count(1) > 60 then 'pv60+' " +
        "      else 'other' end pv_type " +
        "from commerce.access_log " +
        "where event='e_pv' " +
        "group by from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd'), " +
        "    platform, " +
        "    uuid " +
        "order by date asc")
    // 1.2 每个PV下对应着多少用户数
    hiveContext.sql("drop table if exists commerce.stats_view_depth_time_platform_user_count")
    hiveContext.sql("create table commerce.stats_view_depth_time_platform_user_count " +
        "as " +
        "select date,platform,pv_type,count(pv_type) count " +
        "from commerce.stats_view_depth_time_platform_user " +
        "group by date,platform,pv_type")
    // 1.3 多行转一行，同一维度合并
    hiveContext.sql("drop table if exists commerce.stats_view_depth_kpi_user")
    hiveContext.sql("create table commerce.stats_view_depth_kpi_user " +
        "as " +
        "select date,platform,concat_ws(',', collect_set(concat(pv_type, '=', count))) pvs " +
        "from commerce.stats_view_depth_time_platform_user_count " +
        "group by date,platform")
    
    
    // 2.1 根据时间，平台，用户 分组，统计pageView的个数
    hiveContext.sql("drop table if exists commerce.stats_view_depth_time_platform_session")
    hiveContext.sql("create table commerce.stats_view_depth_time_platform_session " +
        "as " +
        "select from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd') date, " +
        "    platform, " +
        "    session_id, " +
        "    count(1) pv, " +
        "    case when count(1) <= 0 then 'other' " +
        "      when count(1) = 1 then 'pv1' " +
        "      when count(1) = 2 then 'pv2' " +
        "      when count(1) = 3 then 'pv3' " +
        "      when count(1) = 4 then 'pv4' " +
        "      when count(1) <= 10 then 'pv5_10' " +
        "      when count(1) <= 30 then 'pv10_30' " +
        "      when count(1) <= 60 then 'pv30_60' " +
        "      when count(1) > 60 then 'pv60+' " +
        "      else 'other' end pv_type " +
        "from commerce.access_log " +
        "where event='e_pv' " +
        "group by from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd'),  " +
        "    platform, " +
        "    session_id " +
        "order by date asc")
    // 2.2 每个PV下对应着多少用户数
    hiveContext.sql("drop table if exists commerce.stats_view_depth_time_platform_session_count")
    hiveContext.sql("create table commerce.stats_view_depth_time_platform_session_count " +
        "as " +
        "select date,platform,pv_type,count(pv_type) count " +
        "from commerce.stats_view_depth_time_platform_session " +
        "group by date,platform,pv_type")
    // 2.3 多行转一行，同一维度合并
    hiveContext.sql("drop table if exists commerce.stats_view_depth_kpi_session")
    hiveContext.sql("create table commerce.stats_view_depth_kpi_session " +
        "as " +
        "select date,platform,concat_ws(',', collect_set(concat(pv_type, '=', count))) pvs " +
        "from commerce.stats_view_depth_time_platform_session_count " +
        "group by date,platform")
    
    
  
    val userDF = hiveContext.sql("select * from commerce.stats_view_depth_kpi_user")
    val sessionDF = hiveContext.sql("select * from commerce.stats_view_depth_kpi_session")
  
    
    val statsViewDepthDao = new StatsViewDepthDao()
    val dimensionDao = new DimensionDao()
    
    /** 1. 用户维度下的浏览深度 */
    userDF.collect().foreach(row => {
      val date = row.getString(0)
      val platform = row.getString(1)
      val pvs = row.getString(2).split(",")
      var countsMap = new mutable.HashMap[String, Int]()
      pvs.foreach(i => {
        val arr = i.split("=")
        countsMap += (arr(0) -> arr(1).toInt)
      })

      addKpi(((date, platform), countsMap.toMap), KpiEnum.ACTIVITY_USER)
    })
  
    /** 2. 会话维度下的浏览深度 */
    sessionDF.collect().foreach(row => {
      val date = row.getString(0)
      val platform = row.getString(1)
      val pvs = row.getString(2).split(",")
      var countsMap = new mutable.HashMap[String, Int]()
      pvs.foreach(i => {
        val arr = i.split("=")
        countsMap += (arr(0) -> arr(1).toInt)
      })

      addKpi(((date, platform), countsMap.toMap), KpiEnum.SESSION_COUNT)
    })
  
  
    /**
      * 获取维度相关信息
      * @param item
      * @return
      */
    def getDimensionIds(item: (String, String, String)): (Int, Int, Int) = {
      val date = item._1 // 时间
      val platformName = item._2 // 平台名称
      val kpiName = item._3 // Kpi名称
    
      // 时间维度id
      val timestamp = TimeUtils.parseString2Long(date, "yyyy-MM-dd")
      val dimensionDate = new DimensionDate(timestamp, DateEnum.DAY)// 设置为以天为单位的时间维度
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
      * @param item
      * @param kpiType
      */
    def addKpi(item: ((String, String), Map[String, Int]), kpiType: KpiEnum): Unit = {
      // 计算相关维度
      val ids = getDimensionIds(item._1._1, item._1._2, kpiType.getName)
    
      // 时间维度id
      val dimensionDateId = ids._1
      // 平台维度id
      val dimensionPlatformId = ids._2
      // Kpi维度id
      val dimensionKpiId = ids._3
    
      // 将统计数据写入数据库
      statsViewDepthDao.addKpiCount(dimensionDateId, dimensionPlatformId, dimensionKpiId, item._2)
    }
    
    
  
    sc.stop()
    
  }
  
}
