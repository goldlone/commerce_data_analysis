package cn.goldlone.commerce.dao

import cn.goldlone.commerce.utils.DBUtil

/**
  *
  * @author Created by CN on 2018/12/5/0005 17:29 .
  */
class StatsDeviceBrowserDao {

  val dbUtil = new DBUtil()
  
  private val queryExistsSql = "select total_install_users " +
      "from stats_device_browser " +
      "where date_dimension_id=? and " +
      "   platform_dimension_id=? and " +
      "   browser_dimension_id=?"
  
  /**
    * 获取某一时间、浏览器、平台维度下的总用户数量
    * @param dimensionDateId 日期维度id
    * @param dimensionBrowserId 浏览器维度id
    * @param dimensionPlatformId 平台维度id
    * @return 总用户数
    */
  def getTotalInstallUserCount(dimensionDateId: Int,
                               dimensionBrowserId: Int,
                               dimensionPlatformId: Int): Int = {
    var totalInstallUserCount = 0
    // 1. 查询昨天的用户数
    var sql = "select total_install_users " +
        "from stats_device_browser " +
        "where date_dimension_id=? and " +
        "   platform_dimension_id=? and " +
        "   browser_dimension_id=?"
    var rs = dbUtil.executeQuery(sql, Array(dimensionDateId,
      dimensionPlatformId, dimensionBrowserId))
    
    // 2. 如果有则返回，否则返回 0
    if(rs.next()) {
      totalInstallUserCount = rs.getInt(1)
    }
    
    totalInstallUserCount
  }
  
  
  /**
    * 获取某一时间、浏览器、平台维度下的总会员数量
    * @param dimensionDateId 日期维度id
    * @param dimensionBrowserId 浏览器维度id
    * @param dimensionPlatformId 平台维度id
    * @return 总会员数
    */
  def getTotalMemberCount(dimensionDateId: Int,
                          dimensionBrowserId: Int,
                          dimensionPlatformId: Int): Int = {
    
    var totalInstallUserCount = 0
    // 1. 查询昨天的用户数
    var sql = "select total_members " +
        "from stats_device_browser " +
        "where date_dimension_id=? and " +
        "   platform_dimension_id=? and " +
        "   browser_dimension_id=?"
    var rs = dbUtil.executeQuery(sql, Array(dimensionDateId,
      dimensionPlatformId, dimensionBrowserId))
    
    // 2. 如果有则返回，否则返回 0
    if(rs.next()) {
      totalInstallUserCount = rs.getInt(1)
    }
    
    totalInstallUserCount
  }
  
  
  
  /**
    * 添加活跃用户数量
    * @param dimensionDateId 日期维度id
    * @param dimensionBrowserId 浏览器维度id
    * @param dimensionPlatformId 平台维度id
    * @param activeUserCount 活跃用户数量
    */
  def addActiveUserCount(dimensionDateId: Int,
                         dimensionBrowserId: Int,
                         dimensionPlatformId: Int,
                         activeUserCount: Int): Unit = {
    
    val querySql = this.queryExistsSql
    val updateSql = "update stats_device_browser " +
        "set active_users = active_users + ? " +
        "where date_dimension_id = ? and " +
        "   platform_dimension_id = ? and " +
        "   browser_dimension_id = ?"
      
    val insertSql = "insert " +
        "into stats_device_browser(date_dimension_id, " +
        "     platform_dimension_id, browser_dimension_id, " +
        "     active_users, created) " +
        "values(?, ?, ?, ?, now())"
    
    val queryArgs = Array[Any](dimensionDateId, dimensionPlatformId, dimensionBrowserId)
    val updateArgs = Array[Any](activeUserCount, dimensionDateId, dimensionPlatformId, dimensionBrowserId)
    val insertArgs = Array[Any](dimensionDateId, dimensionPlatformId, dimensionBrowserId, activeUserCount)
    
    dbUtil.existsUpdateElseInsert(List(querySql, updateSql, insertSql), List(queryArgs, updateArgs, insertArgs))
  }
  
  
  /**
    * 添加新增、总用户数量
    * @param dimensionDateId 日期维度id
    * @param dimensionBrowserId 浏览器维度id
    * @param dimensionPlatformId 平台维度id
    * @param totalInstallUserCount 总用户数量
    * @param newInstallUserCount 新增用户数
    */
  def addNewInstallUserCount(dimensionDateId: Int,
                             dimensionBrowserId: Int,
                             dimensionPlatformId: Int,
                             totalInstallUserCount: Int,
                             newInstallUserCount: Int): Unit = {
    
    val querySql = this.queryExistsSql
    val updateSql = "update stats_device_browser " +
        "set total_install_users = total_install_users + ?, " +
        "   new_install_users = new_install_users + ? " +
        "where date_dimension_id=? and " +
        "   platform_dimension_id=? and " +
        "   browser_dimension_id=?"
    val insertSql = "insert " +
        "into stats_device_browser(date_dimension_id, " +
        "   platform_dimension_id, browser_dimension_id, " +
        "   total_install_users, new_install_users, created) " +
        "values(?, ?, ?, ?, ?, now())"
    
    val queryArgs = Array[Any](dimensionDateId, dimensionPlatformId, dimensionBrowserId)
    val updateArgs = Array[Any](totalInstallUserCount, newInstallUserCount, dimensionDateId, dimensionPlatformId, dimensionBrowserId)
    val insertArgs = Array[Any](dimensionDateId, dimensionPlatformId, dimensionBrowserId, totalInstallUserCount, newInstallUserCount)
  
    dbUtil.existsUpdateElseInsert(List(querySql, updateSql, insertSql), List(queryArgs, updateArgs, insertArgs))
  }
  
  
  /**
    * 添加活跃会员数量
    * @param dimensionDateId 日期维度id
    * @param dimensionBrowserId 浏览器维度id
    * @param dimensionPlatformId 平台维度id
    * @param activeMemberCount 活跃会员数量
    */
  def addActiveMemberCount(dimensionDateId: Int,
                           dimensionBrowserId: Int,
                           dimensionPlatformId: Int,
                           activeMemberCount: Int): Unit = {
    val querySql = queryExistsSql
    val updateSql = "update stats_device_browser " +
        "set active_members = active_members + ? " +
        "where date_dimension_id = ? and " +
        "   platform_dimension_id = ? and " +
        "   browser_dimension_id = ?"
    val insertSql = "insert " +
        "into stats_device_browser(date_dimension_id, " +
        "     platform_dimension_id, browser_dimension_id, " +
        "     active_members, created) " +
        "values(?, ?, ?, ?, now())"
  
    val queryArgs = Array[Any](dimensionDateId, dimensionPlatformId, dimensionBrowserId)
    val updateArgs = Array[Any](activeMemberCount, dimensionDateId, dimensionPlatformId, dimensionBrowserId)
    val insertArgs = Array[Any](dimensionDateId, dimensionPlatformId, dimensionBrowserId, activeMemberCount)
    
    dbUtil.existsUpdateElseInsert(List(querySql, updateSql, insertSql), List(queryArgs, updateArgs, insertArgs))
  }
  
  
  
  /**
    * 添加新增、总会员数量
    * @param dimensionDateId 日期维度id
    * @param dimensionBrowserId 浏览器维度id
    * @param dimensionPlatformId 平台维度id
    * @param totalMemberCount 总会员数量
    * @param activeMemberCount 新增会员数量
    */
  def addNewMemberUserCount(dimensionDateId: Int,
                            dimensionBrowserId: Int,
                            dimensionPlatformId: Int,
                            totalMemberCount: Int,
                            activeMemberCount: Int): Unit = {
  
    val querySql = queryExistsSql
    val updateSql = "update stats_device_browser " +
        "set total_members = total_members + ?, " +
        "   new_members = new_members + ? " +
        "where date_dimension_id=? and " +
        "   platform_dimension_id=? and " +
        "   browser_dimension_id=?"
    val insertSql = "insert " +
        "into stats_device_browser(date_dimension_id, " +
        "   platform_dimension_id, browser_dimension_id, " +
        "   total_members, new_members, created) " +
        "values(?, ?, ?, ?, ?, now())"
  
    val queryArgs = Array[Any](dimensionDateId, dimensionPlatformId, dimensionBrowserId)
    val updateArgs = Array[Any](totalMemberCount, activeMemberCount, dimensionDateId, dimensionPlatformId, dimensionBrowserId)
    val insertArgs = Array[Any](dimensionDateId, dimensionPlatformId, dimensionBrowserId, totalMemberCount, activeMemberCount)
    
    dbUtil.existsUpdateElseInsert(List(querySql, updateSql, insertSql), List(queryArgs, updateArgs, insertArgs))
  }
  
  
  
  /**
    * 添加会话的数量和总长度
    * @param dimensionDateId 日期维度id
    * @param dimensionBrowserId 浏览器维度id
    * @param dimensionPlatformId 平台维度id
    * @param sessionCount session会话总数
    * @param sessionLength session会话总长度
    */
  def addSessionCount(dimensionDateId: Int,
                      dimensionBrowserId: Int,
                      dimensionPlatformId: Int,
                      sessionCount: Int,
                      sessionLength: Int): Unit = {
  
    val querySql = queryExistsSql
    val updateSql = "update stats_device_browser " +
        "set sessions = sessions + ?, " +
        "   sessions_length = sessions_length + ? " +
        "where date_dimension_id=? and " +
        "   platform_dimension_id=? and " +
        "   browser_dimension_id=?"
    val insertSql = "insert " +
        "into stats_device_browser(date_dimension_id, " +
        "   platform_dimension_id, browser_dimension_id, " +
        "   sessions, sessions_length, created) " +
        "values(?, ?, ?, ?, ?, now())"
  
    val queryArgs = Array[Any](dimensionDateId, dimensionPlatformId, dimensionBrowserId)
    val updateArgs = Array[Any](sessionCount, sessionLength, dimensionDateId, dimensionPlatformId, dimensionBrowserId)
    val insertArgs = Array[Any](dimensionDateId, dimensionPlatformId, dimensionBrowserId, sessionCount, sessionLength)
  
    dbUtil.existsUpdateElseInsert(List(querySql, updateSql, insertSql), List(queryArgs, updateArgs, insertArgs))
  
  }
  
  
  /**
    * 添加pv数量
    * @param dimensionDateId 日期维度id
    * @param dimensionBrowserId 浏览器维度id
    * @param dimensionPlatformId 平台维度id
    * @param pageViewCount pv数量
    */
  def addPageViewCount(dimensionDateId: Int,
                       dimensionBrowserId: Int,
                       dimensionPlatformId: Int,
                       pageViewCount: Int): Unit = {
    val querySql = queryExistsSql
    val updateSql = "update stats_device_browser " +
        "set pv = pv + ? " +
        "where date_dimension_id=? and " +
        "   platform_dimension_id=? and " +
        "   browser_dimension_id=?"
    val insertSql = "insert " +
        "into stats_device_browser(date_dimension_id, " +
        "   platform_dimension_id, browser_dimension_id, " +
        "   pv, created) " +
        "values(?, ?, ?, ?, now())"
  
    val queryArgs = Array[Any](dimensionDateId, dimensionPlatformId, dimensionBrowserId)
    val updateArgs = Array[Any](pageViewCount, dimensionDateId, dimensionPlatformId, dimensionBrowserId)
    val insertArgs = Array[Any](dimensionDateId, dimensionPlatformId, dimensionBrowserId, pageViewCount)
  
    dbUtil.existsUpdateElseInsert(List(querySql, updateSql, insertSql), List(queryArgs, updateArgs, insertArgs))
  }
  
}
