package cn.goldlone.commerce.etl.utils;

import cn.goldlone.commerce.etl.common.EventLogConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * 处理收集的日志数据工具类
 * @author Created by CN on 2018/12/4/0004 14:23 .
 */
public class LogUtil2 {

  //获取log4j的日志打印对象logger对象
  public static Logger logger = Logger.getLogger(LogUtil2.class);


  /**
   * 处理logText数据，返回map对象
   * 如果logText为empty，则返回null。
   * @param logText
   * @return
   */
  public static Map<String,String> handleLog(String logText){
    Map<String, String> clientInfo = new HashMap<String, String>();
    //判断logText是否为空
    if(StringUtils.isNotBlank(logText.trim())){
      String [] splits = logText.split(EventLogConstants.LOG_SEPARATOR);
      //判断切分后的长度是否为4 (千万不要去循环)
      if(splits.length == 4){
        // 日志格式为: ip^A服务器时间^Ahost^A请求参数
        clientInfo.put(EventLogConstants.LOG_IP, splits[0]);
        clientInfo.put(EventLogConstants.LOG_SERVER_TIME, TimeUtils.nginxServerTime2Long(splits[1])+"");

        //获取?分隔符是否存在
//        int index = splits[3].indexOf("?");
//        if(index > -1){

        //获取参数的body,也就是我们收集的数据
        String requestBody = splits[3]/*.substring(index+1)*/;
        //处理参数
        handleParams(requestBody, clientInfo);
        //处理useragent(注意：一定要先解析参数，否则clientinfo中没有userAgent信息)
        handleUserAgent(clientInfo);
        //处理ip
        handleIp(clientInfo);

          //返回组装好的对象
//        } else {
//          //参数格式异常 (不清空会有值影响)
//          clientInfo.clear();
//        }
      }
    }
    return clientInfo;
  }

  /**
   * 解析ip
   * @param clientInfo
   */
  public static void handleIp(Map<String, String> clientInfo){
    //判断clientInfo中是否包含ip的key
    if(clientInfo.containsKey(EventLogConstants.LOG_IP)){
      IP2RegionUtil.RegionInfo info = IP2RegionUtil.parseIP(clientInfo.get(EventLogConstants.LOG_IP));
      if(info != null){
        clientInfo.put(EventLogConstants.LOG_REGION_COUNTRY, info.getCountry());
        clientInfo.put(EventLogConstants.LOG_REGION_PROVINCE, info.getProvince());
        clientInfo.put(EventLogConstants.LOG_REGION_CITY, info.getCity());
      }
    }
  }

  public static void handleUserAgent(Map<String,String> clientInfo){
    //判断clientInfo中是否包含userAgent的key
    if(clientInfo.containsKey(EventLogConstants.LOG_USER_AGENT)){
      UserAgentUtil.UserAgentInfo info = UserAgentUtil.parseUserAgent(clientInfo.get(EventLogConstants.LOG_USER_AGENT));
      //判断是否为空
      if(info != null){
        clientInfo.put(EventLogConstants.LOG_OS_NAME, info.getOsName());
        clientInfo.put(EventLogConstants.LOG_OS_VERSION, info.getOsVersion());
        clientInfo.put(EventLogConstants.LOG_BROWSER_NAME, info.getBrowserName());
        clientInfo.put(EventLogConstants.LOG_BROWSER_VERSION, info.getBrowserVersion());
      }
    }
  }

  /**
   * 处理参数，也就是将请求参数填充到map中
   * @param requestBody
   * @param clientInfo
   */
  public static void handleParams(String requestBody, Map<String,String> clientInfo){
    if(StringUtils.isNotBlank(requestBody)){
      //拆分参数
      String [] params = requestBody.split("&");
      for (String param : params) {
        //查找= 的位置
        int index = param.indexOf("=");
        if(index <= -1){
          logger.warn("参数没法进行解析:"+param+" 参数为:"+requestBody);
          continue;
        }
        //然后再对value进行URL解码
        String key = null;
        String value = null;
        try {
          key = param.substring(0, index);
          value = URLDecoder.decode(param.substring(index+1), "utf-8");
        } catch (UnsupportedEncodingException e) {
          logger.warn("解码操作出现异常");
          continue;
        }
        //如果获取出来的key 和value都不空，则填充
        if(StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)){
          clientInfo.put(key, value);
        }
      }
    }
  }


}
