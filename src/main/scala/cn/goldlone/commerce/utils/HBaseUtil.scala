package cn.goldlone.commerce.utils

import java.util.UUID

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
  *
  * @author Created by CN on 2018/11/20/0020 14:23 .
  */
class HBaseUtil {

  private val conf = HBaseConfiguration.create()
  conf.addResource("config/hbase-site.xml")

  private val connection = ConnectionFactory.createConnection(conf)

  def getTable(tableName: String): Table = {
    val tableNameBytes: TableName = TableName.valueOf(Bytes.toBytes(tableName))
    connection.getTable(tableNameBytes)
  }

}


object HBaseUtil {

  def main(args: Array[String]): Unit = {
    val hBaseUtil = new HBaseUtil
  
    val table = hBaseUtil.getTable("commerce:access_log")
    
    val rowkey = Bytes.toBytes(System.currentTimeMillis().toString + "-" + UUID.randomUUID())
    val family = Bytes.toBytes("info")
    val qualifier = Bytes.toBytes("name")
    val value = Bytes.toBytes("Chrome")
    val put = new Put(rowkey)
    put.addColumn(family, qualifier, value)
  
    table.put(put)
  }

}
