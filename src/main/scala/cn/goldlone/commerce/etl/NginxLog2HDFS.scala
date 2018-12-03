package cn.goldlone.commerce.etl

import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util.Locale

import cn.goldlone.commerce.etl.utils.{IP2RegionUtil, UserAgentUtil}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  *
  * @author Created by CN on 2018/12/2/0002 22:27 .
  */
object NginxLog2HDFS {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("readFlumeNginx")
    val sc = new SparkContext(conf)
    System.setProperty("user.name", "hadoop")
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // 读取2018-12-03当天的访问日志
    val rdd = sc.textFile("hdfs://hh:9000/data/commerce/nginx/2018/12/03", 1)

    val sdf1: SimpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
//    val sdf2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val accessRdd = rdd.map(item => {
      var arr = item.split("\t")
      var remote_addr = arr(0) // 请求IP
      remote_addr = "111.203.254.34"
      val remote_user = arr(1) // 请求用户 - 忽略
      val time_local = sdf1.parse(arr(2)).getTime // 请求时间
      val request = arr(3) // 请求方式 地址 协议
      val status = arr(4) // 状态码
      val body_bytes_sent = arr(5) // 传输的字节数 - 忽略
      val http_referer = arr(6) // 从哪个页面跳转过来
      val http_user_agent = arr(7) // user-agent
      val http_x_forwarded_for = arr(8) // ?? - 忽略
      
      val tmpRequestArr = request.split(" ")
      val requestMethod = tmpRequestArr(0)
      val requestUrl = URLDecoder.decode(tmpRequestArr(1), "utf-8")
      val requestProtocol = tmpRequestArr(2)

      
      // 拼接单行访问记录，以 "\t" 分割列
      val builder = new mutable.StringBuilder()
      builder.append(remote_addr).append("\t")
  
      // 解析IP为地域信息
      val regionInfo = IP2RegionUtil.parseIP(remote_addr)
      if(regionInfo == null) {
        // 无法解析地域信息时
        builder.append("null").append("\t")
            .append("null").append("\t")
            .append("null").append("\t")
      } else {
        // 解析地域成功
        builder.append(regionInfo.getCountry).append("\t")
            .append(regionInfo.getProvince).append("\t")
            .append(regionInfo.getCity).append("\t")
      }
      
      builder.append(time_local).append("\t")   // 请求时间
          .append(requestMethod).append("\t")   // 请求方式
          .append(requestProtocol).append("\t") // 请求协议
          .append(requestUrl).append("\t")      // 请求URL
          .append(status).append("\t")          // 请求状态码
          .append(http_referer).append("\t")    // 前置链接
      
      val userAgentInfo = UserAgentUtil.parseUserAgent(http_user_agent)
      if(userAgentInfo == null) {
        builder.append("null").append("\t")
            .append("null").append("\t")
            .append("null")
      } else {
        builder.append(userAgentInfo.getOsName).append("\t")    // 操作系统名称
            .append(userAgentInfo.getOsVersion).append("\t")    // 操作系统版本
            .append(userAgentInfo.getBrowserName).append("\t")  // 浏览器名称
            .append(userAgentInfo.getBrowserVersion)            // 浏览器版本
      }
      
      builder.toString()
    }).cache()
  
    // 保存至HDFS
    accessRdd.repartition(1)
        .saveAsTextFile("hdfs://hh:9000/data/commerce/etl/nginx/"
            +String.valueOf(System.currentTimeMillis()))

    sc.stop()
  }


}
