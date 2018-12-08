package cn.goldlone.commerce.dao

import cn.goldlone.commerce.utils.DBUtil

/**
  *
  * @author Created by CN on 2018/12/8/0008 15:09 .
  */
class StatsViewDepthDao {
  
  private val dbUtil = new DBUtil()
  
  /**
    * 添加KPI指标
    * @param dimensionDateId
    * @param dimensionPlatformId
    * @param dimensionKpiId
    * @param countsMap
    */
  def addKpiCount(dimensionDateId: Int,
                  dimensionPlatformId: Int,
                  dimensionKpiId: Int,
                  countsMap: Map[String, Int]): Unit = {
    
    val sql = "insert " +
        "into stats_view_depth(date_dimension_id, " +
        "     platform_dimension_id, kpi_dimension_id, " +
        "     pv1, pv2, pv3, pv4, pv5_10, pv10_30, " +
        "     pv30_60, `pv60+`, created) " +
        "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now()) " +
        "on duplicate key update pv1=?, pv2=?, pv3=?, " +
        "     pv4=?, pv5_10=?, pv10_30=?, pv30_60=?, `pv60+`=?"
    
    val pv1 = countsMap.getOrElse("pv1", 0)
    val pv2 = countsMap.getOrElse("pv2", 0)
    val pv3 = countsMap.getOrElse("pv3", 0)
    val pv4 = countsMap.getOrElse("pv4", 0)
    val pv5_10 = countsMap.getOrElse("pv5_10", 0)
    val pv10_30 = countsMap.getOrElse("pv10_30", 0)
    val pv30_60 = countsMap.getOrElse("pv30_60", 0)
    val pv60_plus = countsMap.getOrElse("pv60+", 0)
    
    val args = Array[Any](dimensionDateId, dimensionPlatformId,
      dimensionKpiId, pv1, pv2, pv3, pv4, pv5_10, pv10_30, pv30_60,
      pv60_plus, pv1, pv2, pv3, pv4, pv5_10, pv10_30, pv30_60, pv60_plus)
   
    dbUtil.executeInsertOrUpdate(sql, args)
  }



}


