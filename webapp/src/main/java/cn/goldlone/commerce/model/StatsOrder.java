package cn.goldlone.commerce.model;

/**
 * @author Created by CN on 2018/12/12/0012 14:59 .
 */
public class StatsOrder {

  private Integer platform_dimension_id;

  private Integer date_dimension_id;

  private Integer currency_type_dimension_id;

  private Integer payment_type_dimension_id;

  private Integer orders;

  private Integer success_orders;

  private Integer refund_orders;

  private Integer order_amount;

  private Integer revenue_amount;

  private Integer refund_amount;

  private Integer total_revenue_amount;

  private Integer total_refund_amount;

  private Integer created;


  public Integer getPlatform_dimension_id() {
    return platform_dimension_id;
  }

  public void setPlatform_dimension_id(Integer platform_dimension_id) {
    this.platform_dimension_id = platform_dimension_id;
  }

  public Integer getDate_dimension_id() {
    return date_dimension_id;
  }

  public void setDate_dimension_id(Integer date_dimension_id) {
    this.date_dimension_id = date_dimension_id;
  }

  public Integer getCurrency_type_dimension_id() {
    return currency_type_dimension_id;
  }

  public void setCurrency_type_dimension_id(Integer currency_type_dimension_id) {
    this.currency_type_dimension_id = currency_type_dimension_id;
  }

  public Integer getPayment_type_dimension_id() {
    return payment_type_dimension_id;
  }

  public void setPayment_type_dimension_id(Integer payment_type_dimension_id) {
    this.payment_type_dimension_id = payment_type_dimension_id;
  }

  public Integer getOrders() {
    return orders;
  }

  public void setOrders(Integer orders) {
    this.orders = orders;
  }

  public Integer getSuccess_orders() {
    return success_orders;
  }

  public void setSuccess_orders(Integer success_orders) {
    this.success_orders = success_orders;
  }

  public Integer getRefund_orders() {
    return refund_orders;
  }

  public void setRefund_orders(Integer refund_orders) {
    this.refund_orders = refund_orders;
  }

  public Integer getOrder_amount() {
    return order_amount;
  }

  public void setOrder_amount(Integer order_amount) {
    this.order_amount = order_amount;
  }

  public Integer getRevenue_amount() {
    return revenue_amount;
  }

  public void setRevenue_amount(Integer revenue_amount) {
    this.revenue_amount = revenue_amount;
  }

  public Integer getRefund_amount() {
    return refund_amount;
  }

  public void setRefund_amount(Integer refund_amount) {
    this.refund_amount = refund_amount;
  }

  public Integer getTotal_revenue_amount() {
    return total_revenue_amount;
  }

  public void setTotal_revenue_amount(Integer total_revenue_amount) {
    this.total_revenue_amount = total_revenue_amount;
  }

  public Integer getTotal_refund_amount() {
    return total_refund_amount;
  }

  public void setTotal_refund_amount(Integer total_refund_amount) {
    this.total_refund_amount = total_refund_amount;
  }

  public Integer getCreated() {
    return created;
  }

  public void setCreated(Integer created) {
    this.created = created;
  }
}
