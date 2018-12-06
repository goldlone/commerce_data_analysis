package cn.goldlone.commerce.model

import java.util.{Calendar, Date}

import cn.goldlone.commerce.etl.common.DateEnum

/**
  *
  * @author Created by CN on 2018/12/5/0005 11:34 .
  */
class DimensionDate extends BaseDimension {
  
  var id: Int = _
  
  var year: Int = _
  
  var season: Int = _
  
  var month: Int = _
  
  var week: Int = _
  
  var day: Int = _
  
  var calendar: Date = _
  
  var dateType: DateEnum = DateEnum.DAY
  
  def this(timestamp: Long, dateType: DateEnum) {
    this()
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(timestamp)
    this.year = cal.get(Calendar.YEAR)
    this.month = cal.get(Calendar.MONTH)
    if(this.month % 3 == 0)
      this.season = this.month / 3
    else
      this.season = this.month / 3 + 1
    
    this.week = cal.get(Calendar.WEEK_OF_YEAR)
    this.day = cal.get(Calendar.DAY_OF_MONTH)
    this.calendar = cal.getTime
    this.dateType = dateType
  }
  
  def this(year:Int, season:Int, month:Int, week:Int, day:Int, calendar:Date, dateType:DateEnum) {
    this()
    this.year = year
    this.season = season
    this.month = month
    this.week = week
    this.day = day
    this.calendar = calendar
    this.dateType = dateType
  }
  
}
