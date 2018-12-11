package cn.goldlone.commerce.etl.spark

import java.text.SimpleDateFormat

import cn.goldlone.commerce.etl.common.EventLogConstants
import cn.goldlone.commerce.etl.utils.{LogUtil, TimeUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 清洗数据后导入HDFS
  * @author Created by CN on 2018/12/2/0002 22:27 .
  */
object NginxLog2HDFS {

  def main(args: Array[String]): Unit = {

    System.setProperty("user.name", "hadoop")
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://hh:9000")
    val fs = FileSystem.get(hadoopConf)

    var date: String = null

    for(i <- args.indices) {
      if(args(i).equals("-d")) {
        if(i+1 < args.length)
        date = args(i+1)
      }
    }
    
    if(date == null) {
      date = TimeUtils.getYesterday("yyyy-MM-dd")
    }
    
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
    val sdf2 = new SimpleDateFormat("yyyy/MM/dd")
    date = sdf2.format(sdf1.parse(date))
    
    val inPath = "hdfs://hh:9000/data/commerce/nginx/" + date
    val outPath = "hdfs://hh:9000/data/commerce/etl/" + date
    
    println(s"输入路径: $inPath")
    println(s"输出路径: $outPath")
  
    // 检测输入目录是否存在
    if(!fs.exists(new Path(inPath))) {
      println("输入目录不存在...")
      System.exit(1)
    }
    
    // 删除输出目录
    fs.delete(new Path(outPath), true)
  
    // 初始化SparkContext
//    val conf = new SparkConf().setMaster("local[*]").setAppName("nginx-log-etl-hdfs")
    val conf = new SparkConf().setAppName("nginx-log-etl-hdfs")
    val sc = new SparkContext(conf)
    
    // 读取日志信息
    val logRdd = sc.textFile(inPath)
    // 解析日志信息
    val accessRdd: RDD[mutable.HashMap[String, String]] = logRdd.map(LogUtil.handleLog).filter(_ != null)
    // 保存至HDFS
    accessRdd.mapPartitions(it => {
      it.map(item => {
        val builder = new mutable.StringBuilder()
        builder.append(item.getOrElse(EventLogConstants.LOG_IP, "null")).append("\u0001")             // IP
            .append(item.getOrElse(EventLogConstants.LOG_SERVER_TIME, "null")).append("\u0001")       // 服务器时间
            .append(item.getOrElse(EventLogConstants.LOG_EVENT, "null")).append("\u0001")             // 事件类型
            .append(item.getOrElse(EventLogConstants.LOG_PLATFORM, "null")).append("\u0001")          // 平台
            .append(item.getOrElse(EventLogConstants.LOG_SDK, "null")).append("\u0001")               // SDK类型
            .append(item.getOrElse(EventLogConstants.LOG_REGION_COUNTRY, "null")).append("\u0001")    // 地域-国家
            .append(item.getOrElse(EventLogConstants.LOG_REGION_PROVINCE, "null")).append("\u0001")   // 地域-省份
            .append(item.getOrElse(EventLogConstants.LOG_REGION_CITY, "null")).append("\u0001")       // 地域-城市
            .append(item.getOrElse(EventLogConstants.LOG_BROWSER_NAME, "null")).append("\u0001")      // 浏览器名称
            .append(item.getOrElse(EventLogConstants.LOG_BROWSER_VERSION, "null")).append("\u0001")   // 浏览器版本
            .append(item.getOrElse(EventLogConstants.LOG_OS_NAME, "null")).append("\u0001")           // 操作系统名称
            .append(item.getOrElse(EventLogConstants.LOG_OS_VERSION, "null")).append("\u0001")        // 操作系统版本
            .append(item.getOrElse(EventLogConstants.LOG_UUID, "null")).append("\u0001")              // UUID
            .append(item.getOrElse(EventLogConstants.LOG_LANG, "null")).append("\u0001")              // 语言
            .append(item.getOrElse(EventLogConstants.LOG_MEMBER_ID, "null")).append("\u0001")         // 会员号
            .append(item.getOrElse(EventLogConstants.LOG_SESSION_ID, "null")).append("\u0001")        // 会话ID
            .append(item.getOrElse(EventLogConstants.LOG_CLIENT_TIME, "null")).append("\u0001")       // 客户端时间
            .append(item.getOrElse(EventLogConstants.LOG_CURRENT_URL, "null")).append("\u0001")       // 当前URL
            .append(item.getOrElse(EventLogConstants.LOG_REFER_URL, "null")).append("\u0001")         // 前置URL
            .append(item.getOrElse(EventLogConstants.LOG_EVENT_CATEGORY, "null")).append("\u0001")    // 事件类别名称
            .append(item.getOrElse(EventLogConstants.LOG_EVENT_ACTION, "null")).append("\u0001")      // 事件action名称
            .append(item.getOrElse(EventLogConstants.LOG_EVENT_DURATION, "null")).append("\u0001")    // 事件持续时长
            
        // 事件属性
        var count = 0
        item.keySet.foreach(key => {
          if(key.startsWith("kv_")) {
            builder.append(item(key) + ",")
            count += 1
          }
        })
        if(count == 0) {
          builder.append("null").append("\u0001")
        } else {
          builder.deleteCharAt(builder.size - 1)
          builder.append("\u0001")
        }
        
        builder.append(item.getOrElse(EventLogConstants.LOG_ORDER_ID, "null")).append("\u0001")          // 订单Id
            .append(item.getOrElse(EventLogConstants.LOG_ORDER_NAME, "null")).append("\u0001")        // 订单名称
            .append(item.getOrElse(EventLogConstants.LOG_CURRENCY_AMOUNT, "null")).append("\u0001")   // 支付金额
            .append(item.getOrElse(EventLogConstants.LOG_CURRENCY_TYPE, "null")).append("\u0001")     // 支付货币类型
            .append(item.getOrElse(EventLogConstants.LOG_PAYMENT_TYPE, "null"))                       // 支付方式
        
        println(builder.toString())
        builder.toString()
      })
    }).saveAsTextFile(outPath)
    
    
    
    sc.stop()
  }


}
