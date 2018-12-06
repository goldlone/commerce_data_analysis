package cn.goldlone.commerce.etl.spark

import cn.goldlone.commerce.etl.common.EventLogConstants
import cn.goldlone.commerce.etl.utils.LogUtil
import cn.goldlone.commerce.utils.HBaseUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  *
  * @author Created by CN on 2018/12/5/0005 21:20 .
  */
object NginxLog2HBase {
  
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setAppName("nginx-analysis").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
  
  
  
    val logRdd = sc.textFile("hdfs://hh:9000/data/commerce/nginx/2018/12/05")
    val accessRdd: RDD[mutable.HashMap[String, String]] = logRdd.map(LogUtil.handleLog).filter(_ != null).cache()
  
    // 存入HBase
    accessRdd.foreachPartition(iterator => {

      val hBaseUtil = new HBaseUtil
      val table = hBaseUtil.getTable("commerce:access_log")

      iterator.foreach(item => {
        val rowkey = Bytes.toBytes(item(EventLogConstants.LOG_SERVER_TIME) + "-" + item(EventLogConstants.LOG_UUID))
        val family = Bytes.toBytes("info")

        item.foreach(m => {
          val qualifier = Bytes.toBytes(m._1)
          val value = Bytes.toBytes(m._2)
          val put = new Put(rowkey)
          put.addColumn(family, qualifier, value)

          table.put(put)
        })
      })
    })
  
    accessRdd.count()
  }
  
}
