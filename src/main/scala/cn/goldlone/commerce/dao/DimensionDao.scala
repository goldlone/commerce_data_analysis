package cn.goldlone.commerce.dao

import cn.goldlone.commerce.model._
import cn.goldlone.commerce.utils.DBUtil

import scala.collection.mutable

/**
  * 维度信息 数据操作类
  * @author Created by CN on 2018/12/5/0005 20:03 .
  */
class DimensionDao {
  
  // 纬度id缓存器
  private var cache = new mutable.LinkedHashMap[String, Int]()
  
  private val dbUtil = new DBUtil()
  
  
  /**
    * 根据维度属性值查询id
    * @param dimension
    * @return
    */
  def getDimensionIdByValue(dimension: BaseDimension): Int = {
    val cacheKey: String = this.buildCacheKey(dimension)
    if(this.cache.contains(cacheKey)) {
      return this.cache(cacheKey)
    }
    
    var sqls: Array[String] = null
    var args: Array[Any] = null
    
    // 根据类型匹配SQL语句和参数
    dimension match {
      case _: DimensionDate =>      // 日期维度
        sqls = this.buildDateSql()
        val date = dimension.asInstanceOf[DimensionDate]
        args = Array(date.year, date.season, date.month,
          date.week, date.day, date.calendar, date.dateType.name)
        
      case _: DimensionPlatform =>  // 平台维度
        sqls = this.buildPlatformSql()
        val platform = dimension.asInstanceOf[DimensionPlatform]
        args = Array(platform.name)
        
      case _: DimensionBrowser =>   // 浏览器维度
        sqls = this.buildBrowserSql()
        val browser = dimension.asInstanceOf[DimensionBrowser]
        args = Array(browser.name, browser.version)

      case _: DimensionLocation =>  // 地域维度
        sqls = this.buildLocationSql()
        val location = dimension.asInstanceOf[DimensionLocation]
        args = Array(location.country, location.province, location.city)

      case _: DimensionOs =>        // 操作系统维度
        sqls = this.buildOsSql()
        val os = dimension.asInstanceOf[DimensionOs]
        args = Array(os.name, os.version)
        
      case _ => throw new Exception("不支持查询该纬度类型id")
    }
    
    // 在数据库中查询
    val rs1 = dbUtil.executeQuery(sqls(0), args)
    var id = -1
    if(rs1.next()) {
      id = rs1.getInt(1)
    } else {
      val rs2 = dbUtil.insertForGeneratedKeys(sqls(1), args)
      if(rs2.next())
        id = rs2.getInt(1)
    }
    
    if(id == -1) return id
    
    // 将纬度ID添加到缓存中，移除老缓存
    synchronized {
      cache += (cacheKey -> id)
      if(cache.size > 5000)
        cache.remove(cache.head._1)
    }
    
    id
  }
  
  
  /**
    * 创建cache key
    * @param dimension
    * @return
    */
  private def buildCacheKey(dimension: BaseDimension): String = {
    val sb = new StringBuilder
    
    dimension match {
      case _: DimensionDate =>
        val date = dimension.asInstanceOf[DimensionDate]
        sb.append("date_dimension").append(date.year)
            .append(date.season).append(date.month)
            .append(date.week).append(date.day)
            .append(date.dateType)
        
      case _: DimensionPlatform =>
        val platform = dimension.asInstanceOf[DimensionPlatform]
        sb.append("platform_dimension").append(platform.name)
        
      case _: DimensionBrowser =>
        val browser = dimension.asInstanceOf[DimensionBrowser]
        sb.append("browser_dimension").append(browser.name)
            .append(browser.version)
      case _ => throw new Exception("无法创建指定dimension的cachekey: " + dimension.getClass)
    }
    
    sb.toString
  }
  
  
  /**
    * 构建 date dimension 相关SQL
    * @return
    */
  def buildDateSql(): Array[String] = {
    val querySql = "select id " +
        "from dimension_date " +
        "where year=? and " +
        "     season=? and " +
        "     month=? and " +
        "     week=? and " +
        "     day=? and " +
        "     calendar=? and " +
        "     type=?"
    val insertSql = "insert into " +
        "dimension_date(year, season, month, week, day, calendar, type) " +
        "values(?, ?, ?, ?, ?, ?, ?)"
    
    Array(querySql, insertSql)
  }
  
  
  /**
    * 构建 browser dimension 相关SQL
    * @return
    */
  def buildBrowserSql(): Array[String] = {
    val querySql = "select id " +
        "from dimension_browser " +
        "where browser_name=? and browser_version=?"
    val insertSql = "insert into " +
        "dimension_browser(browser_name, browser_version) " +
        "values(?, ?)"
    
    Array(querySql, insertSql)
  }
  
  
  /**
    * 构建 platform dimension 相关SQL
    * @return
    */
  def buildPlatformSql(): Array[String] = {
    val querySql = "select id " +
        "from dimension_platform " +
        "where platform_name=?"
    val insertSql = "insert into dimension_platform(platform_name) values(?)"
    
    Array(querySql, insertSql)
  }
  
  
  /**
    * 构建 location dimension 相关SQL
    * @return
    */
  def buildLocationSql(): Array[String] = {
    val querySql = "select id " +
        "from dimension_location " +
        "where country=? and " +
        "      province=? and " +
        "      city=?"
    val insertSql = "insert into dimension_location(country, province, city) values(?, ?, ?)"
    
    Array(querySql, insertSql)
  }
  
  
  /**
    * 构建 os dimension 相关SQL
    * @return
    */
  def buildOsSql(): Array[String] = {
    val querySql = "select id " +
        "from dimension_os " +
        "where os_name=? and os_version=?"
    val insertSql = "insert into dimension_os(os_name, os_version) values(?, ?)"
    
    Array(querySql, insertSql)
  }
  
  
}
