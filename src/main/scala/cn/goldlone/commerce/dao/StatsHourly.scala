package cn.goldlone.commerce.dao

import cn.goldlone.commerce.utils.DBUtil

/**
  *
  * @author Created by CN on 2018/12/7/0007 9:43 .
  */
class StatsHourly {
  
  
  val dbUtil = new DBUtil()
  
  private val queryExistsSql = "select * " +
      "from stats_hourly " +
      "where date_dimension_id = ? and " +
      "   platform_dimension_id = ? and " +
      "   kpi_dimension_id = ?"
  
  private val updateSql = "update stats_hourly " +
      "set hour_00 = hour_00 + ?, " +
      "    hour_01 = hour_01 + ?, " +
      "    hour_02 = hour_02 + ?, " +
      "    hour_03 = hour_03 + ?, " +
      "    hour_04 = hour_04 + ?, " +
      "    hour_05 = hour_05 + ?, " +
      "    hour_06 = hour_06 + ?, " +
      "    hour_07 = hour_07 + ?, " +
      "    hour_08 = hour_08 + ?, " +
      "    hour_09 = hour_09 + ?, " +
      "    hour_10 = hour_10 + ?, " +
      "    hour_11 = hour_11 + ?, " +
      "    hour_12 = hour_12 + ?, " +
      "    hour_13 = hour_13 + ?, " +
      "    hour_14 = hour_14 + ?, " +
      "    hour_15 = hour_15 + ?, " +
      "    hour_16 = hour_16 + ?, " +
      "    hour_17 = hour_17 + ?, " +
      "    hour_18 = hour_18 + ?, " +
      "    hour_19 = hour_19 + ?, " +
      "    hour_20 = hour_20 + ?, " +
      "    hour_21 = hour_21 + ?, " +
      "    hour_22 = hour_22 + ?, " +
      "    hour_23 = hour_23 + ? " +
      "where date_dimension_id = ? and " +
      "   platform_dimension_id = ? and " +
      "   kpi_dimension_id = ? "
  
  private val insertSql = "insert into " +
      "stats_hourly(platform_dimension_id, date_dimension_id, " +
      "       kpi_dimension_id, hour_00, hour_01, hour_02, " +
      "       hour_03, hour_04, hour_05, hour_06, hour_07, " +
      "       hour_08, hour_09, hour_10, hour_11, hour_12, " +
      "       hour_13, hour_14, hour_15, hour_16, hour_17, " +
      "       hour_18, hour_19, hour_20, hour_21, hour_22, " +
      "       hour_23, created) " +
      "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
      "       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())"
  
  
  /**
    * 添加 KPI 指标
    * @param dimensionDateId 日期维度id
    * @param dimensionPlatformId 平台维度id
    * @param dimensionKpiId kpi维度id
    * @param countsMap 每小时数据(k,v)
    */
  def addKpiCount(dimensionDateId: Int,
                  dimensionPlatformId: Int,
                  dimensionKpiId: Int,
                  countsMap: Map[Int, Int]): Unit = {
    
    val selectArgs = Array[Any](dimensionDateId, dimensionPlatformId, dimensionKpiId)
    
    val updateArgs = Array[Any](countsMap.getOrElse(0, 0),
      countsMap.getOrElse(1, 0),
      countsMap.getOrElse(2, 0),
      countsMap.getOrElse(3, 0),
      countsMap.getOrElse(4, 0),
      countsMap.getOrElse(5, 0),
      countsMap.getOrElse(6, 0),
      countsMap.getOrElse(7, 0),
      countsMap.getOrElse(8, 0),
      countsMap.getOrElse(9, 0),
      countsMap.getOrElse(10, 0),
      countsMap.getOrElse(11, 0),
      countsMap.getOrElse(12, 0),
      countsMap.getOrElse(13, 0),
      countsMap.getOrElse(14, 0),
      countsMap.getOrElse(15, 0),
      countsMap.getOrElse(16, 0),
      countsMap.getOrElse(17, 0),
      countsMap.getOrElse(18, 0),
      countsMap.getOrElse(19, 0),
      countsMap.getOrElse(20, 0),
      countsMap.getOrElse(21, 0),
      countsMap.getOrElse(22, 0),
      countsMap.getOrElse(23, 0),
      dimensionDateId,
      dimensionPlatformId,
      dimensionKpiId)
    
    val insertArgs = Array[Any](dimensionDateId,
      dimensionPlatformId,
      dimensionKpiId,
      countsMap.getOrElse(0, 0),
      countsMap.getOrElse(1, 0),
      countsMap.getOrElse(2, 0),
      countsMap.getOrElse(3, 0),
      countsMap.getOrElse(4, 0),
      countsMap.getOrElse(5, 0),
      countsMap.getOrElse(6, 0),
      countsMap.getOrElse(7, 0),
      countsMap.getOrElse(8, 0),
      countsMap.getOrElse(9, 0),
      countsMap.getOrElse(10, 0),
      countsMap.getOrElse(11, 0),
      countsMap.getOrElse(12, 0),
      countsMap.getOrElse(13, 0),
      countsMap.getOrElse(14, 0),
      countsMap.getOrElse(15, 0),
      countsMap.getOrElse(16, 0),
      countsMap.getOrElse(17, 0),
      countsMap.getOrElse(18, 0),
      countsMap.getOrElse(19, 0),
      countsMap.getOrElse(20, 0),
      countsMap.getOrElse(21, 0),
      countsMap.getOrElse(22, 0),
      countsMap.getOrElse(23, 0))
    
    
    dbUtil.existsUpdateElseInsert(List(queryExistsSql, updateSql, insertSql),
      List(selectArgs, updateArgs, insertArgs))
  }

  
}
