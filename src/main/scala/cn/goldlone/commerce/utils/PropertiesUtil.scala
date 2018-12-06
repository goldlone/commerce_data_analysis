package cn.goldlone.commerce.utils

import java.io.IOException
import java.util.Properties

/**
  *
  * @author Created by CN on 2018/12/5/0005 18:08 .
  */
class PropertiesUtil {
  private var fileName: String = _
  private val properties = new Properties
  
  def this(fileName: String) {
    this()
    this.fileName = fileName
    open()
  }
  
  private def open(): Unit = {
    try
      properties.load(Thread.currentThread.getContextClassLoader.getResourceAsStream(fileName))
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }
  
  /**
    * 根据key取出properties文件中对应的value
    *
    * @param key
    * @return value
    */
  def readPropertyByKey(key: String): String = properties.getProperty(key)
  
  /**
    * 从文件中读取配置后将整个的Properties返回
    *
    * @return properties成员
    */
  def getProperties: Properties = this.properties
  
}
