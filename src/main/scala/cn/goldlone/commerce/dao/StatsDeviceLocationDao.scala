package cn.goldlone.commerce.dao

import cn.goldlone.commerce.utils.DBUtil

/**
  *
  * @author Created by CN on 2018/12/7/0007 15:14 .
  */
class StatsDeviceLocationDao {
  
  val dbUtil = new DBUtil()
  
  /**
    * 添加活跃用户数量
    * @param dimensionDateId 日期维度id
    * @param dimensionPlatformId 平台维度id
    * @param dimensionLocationId 维度id
    * @param activeUserCount 活跃用户数量
    */
  def addActiveUserCount(dimensionDateId: Int,
                         dimensionPlatformId: Int,
                         dimensionLocationId: Int,
                         activeUserCount: Int): Unit = {
    
    val sql = "insert " +
        "into stats_device_location(date_dimension_id, platform_dimension_id, " +
        "     location_dimension_id, active_users, created) " +
        "values(?, ?, ?, ?, now()) " +
        "on duplicate key update active_users = ?"
    
    dbUtil.executeInsertOrUpdate(sql, Array(dimensionDateId, dimensionPlatformId,
      dimensionLocationId, activeUserCount, activeUserCount))
  }
  
  
  /**
    * 添加活跃用户数量
    * @param dimensionDateId 日期维度id
    * @param dimensionPlatformId 平台维度id
    * @param dimensionLocationId 维度id
    * @param sessionCount Session会话总数
    */
  def addSessionCount(dimensionDateId: Int,
                      dimensionPlatformId: Int,
                      dimensionLocationId: Int,
                      sessionCount: Int): Unit = {
    
    val sql = "insert " +
        "into stats_device_location(date_dimension_id, platform_dimension_id, " +
        "     location_dimension_id, sessions, created) " +
        "values(?, ?, ?, ?, now()) " +
        "on duplicate key update sessions = ?"
    
    dbUtil.executeInsertOrUpdate(sql, Array(dimensionDateId, dimensionPlatformId,
      dimensionLocationId, sessionCount, sessionCount))
  }
  
  
  /**
    * 添加跳出会话个数
    * @param dimensionDateId 日期维度id
    * @param dimensionPlatformId 平台维度id
    * @param dimensionLocationId 维度id
    * @param bounceSessionsCount 跳出会话个数
    */
  def addBounceSessionCount(dimensionDateId: Int,
                            dimensionPlatformId: Int,
                            dimensionLocationId: Int,
                            bounceSessionsCount: Int): Unit = {
    
    val sql = "insert " +
        "into stats_device_location(date_dimension_id, platform_dimension_id, " +
        "     location_dimension_id, bounce_sessions, created) " +
        "values(?, ?, ?, ?, now()) " +
        "on duplicate key update bounce_sessions = ?"
    
    dbUtil.executeInsertOrUpdate(sql, Array(dimensionDateId, dimensionPlatformId,
      dimensionLocationId, bounceSessionsCount, bounceSessionsCount))
  }
  
  
  
}
