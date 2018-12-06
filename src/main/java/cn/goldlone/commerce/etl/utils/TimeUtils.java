package cn.goldlone.commerce.etl.utils;

import cn.goldlone.commerce.etl.common.DateEnum;
import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 时间转换工具类
 * @author Created by CN on 2018/12/4/0004 9:33 .
 */
public class TimeUtils {


  /**
   * 时间格式
   */
  public static final String DATE_FORMAT = "yyyy-MM-dd";

  /**
   * 将时间字符串转换为时间戳,如果解析失败返回-1
   * @param input
   * @return
   */
  public static long nginxServerTime2Long(String input){
    Date date = nginxServerTime2Date(input);
    return date == null ? -1L : date.getTime();
  }

  /**
   * 将时间字符串转换为Date,如果时间字符串是empty或者解析失败，则返回null
   * @param input
   * 			格式: 1449410796.976
   * @return
   */
  public static Date nginxServerTime2Date(String input){
    if(StringUtils.isNotBlank(input)){
      try{
        //获取时间值得毫秒数
        long timeStamp = Double.valueOf(Double.valueOf(input.trim())*1000).longValue();
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timeStamp);
        return calendar.getTime();
      } catch(Exception e) {
        //do nothing
      }
    }
    return null;
  }

  /**
   * 判断输入的数据是否为一个有效的时间
   * @param input
   * @return
   */
  public static boolean isValidateRunningDate(String input) {
    Matcher matcher = null;
    boolean result = false;
    //定义判断一个是否为正确时间的正则表达式
    String reg = "^[0-9]{4}-[0-9]{2}-[0-9]{2}$";
    if(input != null && !input.isEmpty()){
      Pattern pattern = Pattern.compile(reg);
      matcher = pattern.matcher(input);
    }
    //如果是matcher为null则代表是空的
    if(matcher != null){
      result = matcher.matches(); //判断是否能匹配上
    }
    return result;
  }

  /**
   * 获取昨天的时间字符串
   * @return
   */
  public static String getYesterday() {
    return getYesterday(DATE_FORMAT);
  }

  /**
   * 获取对应格式的时间字符串
   * @param pattern
   * @return
   */
  public static String getYesterday(String pattern){
    SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DAY_OF_YEAR, -1);
    return sdf.format(calendar.getTime());
  }

  /**
   * 将字符串时间“yyyy-MM-dd”转换成成时间戳
   * @param input
   * @return
   */
  public static long parseString2Long(String input) {

    return parseString2Long(input,DATE_FORMAT);
  }

  /**
   * 将具体格式的时间字符串转换为时间戳
   * @param input
   * @param pattern
   * @return
   */
  public static long parseString2Long(String input,String pattern){
    Date date = null;
    try {
      date = new SimpleDateFormat(DATE_FORMAT).parse(input);
    } catch (ParseException e) {
      throw new RuntimeException("时间解析有问题："+input);
    }
    return date.getTime();
  }


  /**
   * 将时间戳转换为"yyyy-MM-dd"格式时间字符串格式
   * @param input
   * @return
   */
  public static String parseLong2String(long input) {
    return parseLong2String(input, DATE_FORMAT);
  }

  /**
   * 具体将时间戳转换为指定格式的时间字符串
   * @param input
   * @param pattern
   * @return
   */
  public static String parseLong2String(long input, String pattern){
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(input);
    return new SimpleDateFormat(pattern).format(calendar.getTime());
  }

  /**
   * 根据时间戳获取对应的日期信息
   *
   * @param time
   *            时间戳
   * @param type
   *            类型
   * @return
   */
  public static int getDateInfo(long time, DateEnum type) {
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(time);
    if(DateEnum.YEAR.equals(type)){
      return cal.get(Calendar.YEAR);
    }
    if(DateEnum.SEASON.equals(type)){
      //季度信息需要月份才能算出来
      int month = cal.get(Calendar.MONTH) + 1; //默认从0开始
      if(month % 3 == 0){
        return month / 3;
      }
      return month/3 + 1;
    }
    if(DateEnum.MONTH.equals(type)){
      return cal.get(Calendar.MONTH) + 1;
    }
    if(DateEnum.WEEK.equals(type)){
      return cal.get(Calendar.WEEK_OF_YEAR);
    }
    if(DateEnum.DAY.equals(type)){
      return cal.get(Calendar.DAY_OF_MONTH);
    }
    if(DateEnum.HOUR.equals(type)){
      return cal.get(Calendar.HOUR_OF_DAY);
    }
    //如果到这还没有返回，则抛出异常
    throw new RuntimeException("不能从时间戳中获取该时间类型type:"+type.name);
  }

  /**
   * 获取指定时间戳所属周的第一天的时间戳
   * @param time
   * @return
   */
  public static long getFirstDayOfThisWeek(long time) {
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(time);
    cal.set(Calendar.DAY_OF_WEEK, 1);//周第一天
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTimeInMillis(); //返回对应的时间戳即可
  }


//  private static final String NGINX_DATE_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z";
//
//  private static final String DATE_FORM = "yyyy-MM-dd HH:mm:ss";
//
//  public static Long parseTimeStamp(String time) {
//    SimpleDateFormat sdf1 = new SimpleDateFormat(NGINX_DATE_FORMAT, Locale.ENGLISH);
//
//    try {
//      return sdf1.parse(time).getTime();
//    } catch (ParseException e) {
//      e.printStackTrace();
//    }
//
//    return null;
//  }
//
//
//  public static Date parseDate(String time) {
//    Date date = null;
//    SimpleDateFormat sdf = new SimpleDateFormat(NGINX_DATE_FORMAT, Locale.ENGLISH);
//    try {
//      date = sdf.parse(time);
//    } catch (ParseException e) {
//      e.printStackTrace();
//    }
//
//    return date;
//  }
//
//
//  public static String parseDateFormat(String time) {
//    SimpleDateFormat sdf1 = new SimpleDateFormat(NGINX_DATE_FORMAT, Locale.ENGLISH);
//    SimpleDateFormat sdf2 = new SimpleDateFormat(DATE_FORM, Locale.ENGLISH);
//    try {
//      return sdf2.format(sdf1.parse(time));
//    } catch (ParseException e) {
//      e.printStackTrace();
//    }
//
//    return null;
//  }

}
