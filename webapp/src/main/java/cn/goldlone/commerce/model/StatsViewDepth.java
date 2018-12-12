package cn.goldlone.commerce.model;

import java.util.Date;

/**
 * @author Created by CN on 2018/12/12/0012 14:52 .
 */
public class StatsViewDepth {

  private Integer platform_dimension_id;
  private Integer date_dimension_id;
  private Integer kpi_dimension_id;
  private Integer pv1;
  private Integer pv2;
  private Integer pv3;
  private Integer pv4;
  private Integer pv5_10;
  private Integer pv10_30;
  private Integer pv30_60;
  private Integer pv60_plus;
  private Date created;

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

  public Integer getKpi_dimension_id() {
    return kpi_dimension_id;
  }

  public void setKpi_dimension_id(Integer kpi_dimension_id) {
    this.kpi_dimension_id = kpi_dimension_id;
  }

  public Integer getPv1() {
    return pv1;
  }

  public void setPv1(Integer pv1) {
    this.pv1 = pv1;
  }

  public Integer getPv2() {
    return pv2;
  }

  public void setPv2(Integer pv2) {
    this.pv2 = pv2;
  }

  public Integer getPv3() {
    return pv3;
  }

  public void setPv3(Integer pv3) {
    this.pv3 = pv3;
  }

  public Integer getPv4() {
    return pv4;
  }

  public void setPv4(Integer pv4) {
    this.pv4 = pv4;
  }

  public Integer getPv5_10() {
    return pv5_10;
  }

  public void setPv5_10(Integer pv5_10) {
    this.pv5_10 = pv5_10;
  }

  public Integer getPv10_30() {
    return pv10_30;
  }

  public void setPv10_30(Integer pv10_30) {
    this.pv10_30 = pv10_30;
  }

  public Integer getPv30_60() {
    return pv30_60;
  }

  public void setPv30_60(Integer pv30_60) {
    this.pv30_60 = pv30_60;
  }

  public Integer getPv60_plus() {
    return pv60_plus;
  }

  public void setPv60_plus(Integer pv60_plus) {
    this.pv60_plus = pv60_plus;
  }

  public Date getCreated() {
    return created;
  }

  public void setCreated(Date created) {
    this.created = created;
  }
}
