package cn.goldlone.commerce.model;

import java.util.Date;

/**
 * @author Created by CN on 2018/12/12/0012 14:45 .
 */
public class DimensionDate {

  private Integer id;

  private Integer year;

  private Integer season;

  private Integer month;

  private Integer week;

  private Integer day;

  private Date calendar;

  private String type;


  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public Integer getYear() {
    return year;
  }

  public void setYear(Integer year) {
    this.year = year;
  }

  public Integer getSeason() {
    return season;
  }

  public void setSeason(Integer season) {
    this.season = season;
  }

  public Integer getMonth() {
    return month;
  }

  public void setMonth(Integer month) {
    this.month = month;
  }

  public Integer getWeek() {
    return week;
  }

  public void setWeek(Integer week) {
    this.week = week;
  }

  public Integer getDay() {
    return day;
  }

  public void setDay(Integer day) {
    this.day = day;
  }

  public Date getCalendar() {
    return calendar;
  }

  public void setCalendar(Date calendar) {
    this.calendar = calendar;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}
