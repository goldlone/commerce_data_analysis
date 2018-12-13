package cn.goldlone.commerce.model;

import java.util.Date;

/**
 * @author Created by CN on 2018/12/12/0012 14:52 .
 */
public class StatsViewDepth {

  private Integer platformId;
  private Integer dateId;
  private Integer kpiId;
  private Integer pv1;
  private Integer pv2;
  private Integer pv3;
  private Integer pv4;
  private Integer pv5to10;
  private Integer pv10to30;
  private Integer pv30to60;
  private Integer pv60plus;


  public Integer getPlatformId() {
    return platformId;
  }

  public void setPlatformId(Integer platformId) {
    this.platformId = platformId;
  }

  public Integer getDateId() {
    return dateId;
  }

  public void setDateId(Integer dateId) {
    this.dateId = dateId;
  }

  public Integer getKpiId() {
    return kpiId;
  }

  public void setKpiId(Integer kpiId) {
    this.kpiId = kpiId;
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

  public Integer getPv5to10() {
    return pv5to10;
  }

  public void setPv5to10(Integer pv5to10) {
    this.pv5to10 = pv5to10;
  }

  public Integer getPv10to30() {
    return pv10to30;
  }

  public void setPv10to30(Integer pv10to30) {
    this.pv10to30 = pv10to30;
  }

  public Integer getPv30to60() {
    return pv30to60;
  }

  public void setPv30to60(Integer pv30to60) {
    this.pv30to60 = pv30to60;
  }

  public Integer getPv60plus() {
    return pv60plus;
  }

  public void setPv60plus(Integer pv60plus) {
    this.pv60plus = pv60plus;
  }

  @Override
  public String toString() {
    return "StatsViewDepth{" +
            "platformId=" + platformId +
            ", dateId=" + dateId +
            ", kpiId=" + kpiId +
            ", pv1=" + pv1 +
            ", pv2=" + pv2 +
            ", pv3=" + pv3 +
            ", pv4=" + pv4 +
            ", pv5to10=" + pv5to10 +
            ", pv10to30=" + pv10to30 +
            ", pv30to60=" + pv30to60 +
            ", pv60plus=" + pv60plus +
            '}';
  }
}
