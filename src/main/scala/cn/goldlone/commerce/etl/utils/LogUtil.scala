package cn.goldlone.commerce.etl.utils

import java.net.URLDecoder

import cn.goldlone.commerce.etl.common.EventLogConstants
import org.apache.commons.lang.StringUtils

import scala.collection.mutable

/**
  * 解析日志工具类
  * @author Created by CN on 2018/12/5/0005 19:37 .
  */
object LogUtil {
  
  /**
    * 处理单行日志
    * @param line
    * @return
    */
  def handleLog(line: String): mutable.HashMap[String, String] = {
    val fields = line.split(EventLogConstants.LOG_SEPARATOR)
    
    if(fields.length != 4)
      return null
    
    val ip = fields(0)
    val unixTime = fields(1)
    //    val hostname = fields(2)
    val requestParams = fields(3)
    
    var info = new mutable.HashMap[String, String]()
    info += (EventLogConstants.LOG_IP -> ip)
    info += (EventLogConstants.LOG_SERVER_TIME -> TimeUtils.nginxServerTime2Long(unixTime).toString)
    
    handleIp(ip, info)
    handleParams(requestParams, info)
    handleUserAgent(info)
    
    info
  }
  
  
  /**
    * 解析IP为地域信息
    * @param ip
    * @param info
    * @return
    */
  def handleIp(ip: String, info: mutable.HashMap[String, String]): Unit = {
    val option = info.get(EventLogConstants.LOG_IP)
    
    if(option != None) {
      val regionInfo = IP2RegionUtil.parseIP(option.get)
      info += (EventLogConstants.LOG_REGION_COUNTRY -> regionInfo.getCountry)
      info += (EventLogConstants.LOG_REGION_PROVINCE -> regionInfo.getProvince)
      info += (EventLogConstants.LOG_REGION_CITY -> regionInfo.getCity)
    }
    
  }
  
  /**
    * 解析客户端信息
    * @param info
    * @return
    */
  def handleUserAgent(info: mutable.HashMap[String, String]): Unit = {
    val option = info.get(EventLogConstants.LOG_USER_AGENT)
    
    if(option != None) {
      val userAgentInfo = UserAgentUtil.parseUserAgent(option.get)
      info += (EventLogConstants.LOG_OS_NAME -> userAgentInfo.getOsName)
      info += (EventLogConstants.LOG_OS_VERSION -> userAgentInfo.getOsVersion)
      info += (EventLogConstants.LOG_BROWSER_NAME -> userAgentInfo.getBrowserName)
      info += (EventLogConstants.LOG_BROWSER_VERSION -> userAgentInfo.getBrowserVersion)
    }
  }
  
  /**
    * 解析请求参数（不带?）
    * @param requestParams
    * @param info
    */
  def handleParams(requestParams: String, info: mutable.HashMap[String, String]): Unit = {
    if(StringUtils.isNotBlank(requestParams)) {
      val params = requestParams.split("&")
      
      for(param <- params) {
        if(StringUtils.isNotBlank(param)) {
          
          try {
            val index = param.indexOf("=")
            if (index > 0) {
              val key = param.substring(0, index)
              val value = URLDecoder.decode(param.substring(index + 1), "utf-8")
              info += (key -> value)
            }
          } catch {
            case e: Exception => println(e.getMessage)
          }
        }
      }
    }
  }
}
