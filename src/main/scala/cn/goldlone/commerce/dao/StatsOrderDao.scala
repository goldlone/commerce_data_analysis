package cn.goldlone.commerce.dao

import cn.goldlone.commerce.utils.DBUtil

/**
  *
  * @author Created by CN on 2018/12/8/0008 22:44 .
  */
class StatsOrderDao {
  
  val dbUtil = new DBUtil()
  
  
  /**
    * 查询历史的金额信息
    * @param dimensionDateId
    * @param dimensionPlatformId
    * @param dimensionCurrencyTypeId
    * @param dimensionPaymentTypeId
    * @return
    */
  def getTotalRevenueAndRefundAmount(dimensionDateId: Int,
                                     dimensionPlatformId: Int,
                                     dimensionCurrencyTypeId: Int,
                                     dimensionPaymentTypeId: Int): (Int, Int) = {
    val sql = "select total_revenue_amount, total_refund_amount " +
        "from stats_order " +
        "where date_dimension_id=? and " +
        "   platform_dimension_id=? and " +
        "   currency_type_dimension_id=? and " +
        "   payment_type_dimension_id=?"
    
    var res = (0, 0)
    val rs = dbUtil.executeQuery(sql, Array(dimensionDateId, dimensionPlatformId,
            dimensionCurrencyTypeId, dimensionPaymentTypeId))
    
    if(rs.next()) {
      res = (rs.getInt(1), rs.getInt(2))
    }
    
    res
  }
  
  
  /**
    * 添加一条数据
    * @param dimensionDateId
    * @param dimensionPlatformId
    * @param dimensionCurrencyTypeId
    * @param dimensionPaymentTypeId
    * @param orders
    * @param success_orders
    * @param refund_orders
    * @param order_amount
    * @param revenue_amount
    * @param refund_amount
    * @param total_revenue_amount
    * @param total_refund_amount
    */
  def addOne(dimensionDateId: Int,
             dimensionPlatformId: Int,
             dimensionCurrencyTypeId: Int,
             dimensionPaymentTypeId: Int,
             orders: Int,
             success_orders: Int,
             refund_orders: Int,
             order_amount: Int,
             revenue_amount: Int,
             refund_amount: Int,
             total_revenue_amount: Int,
             total_refund_amount: Int): Unit = {
    
    val sql = "insert " +
        "into stats_order(date_dimension_id, platform_dimension_id, " +
        "   currency_type_dimension_id, payment_type_dimension_id, " +
        "   orders, success_orders, refund_orders, order_amount, " +
        "   revenue_amount, refund_amount, total_revenue_amount, " +
        "   total_refund_amount, created) " +
        "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now()) " +
        "on duplicate key update orders=?, success_orders=?, " +
        "   refund_orders=?, order_amount=?, revenue_amount=?, " +
        "   refund_amount=?, total_revenue_amount=?, " +
        "   total_refund_amount=?"
    
    dbUtil.executeInsertOrUpdate(sql, Array(dimensionDateId, dimensionPlatformId,
      dimensionCurrencyTypeId, dimensionPaymentTypeId, orders, success_orders,
      refund_orders, order_amount, revenue_amount, refund_amount, total_revenue_amount,
      total_refund_amount, orders, success_orders, refund_orders, order_amount,
      revenue_amount, refund_amount, total_revenue_amount, total_refund_amount))
  }
  
  
  
}
