package cn.goldlone.commerce.etl.common;


import jdk.nashorn.internal.runtime.GlobalConstants;

/**
 * 日志参数相关常量
 * @author Created by CN on 2018/12/4/0004 13:56 .
 */
public class EventLogConstants {

  // 分割符
  public static final String LOG_SEPARATOR = "\\^A";

  // 默认值
  public static final String DEFAULT_VALUE = "unknown";

  // IP
  public static final String LOG_IP = "ip";

  // 服务器时间
  public static final String LOG_SERVER_TIME = "server_time";


  // 事件类型
  public static final String LOG_EVENT = "en";

  // 版本号
  public static final String LOG_VERSION = "ver";

  // 平台
  public static final String LOG_PLATFORM = "pl";

  // SDK类型
  public static final String LOG_SDK = "sdk";

  // 分辨率
  public static final String LOG_RESOLUTION = "b_rst";

  // 浏览器信息(user-agent)
  public static final String LOG_USER_AGENT = "b_iev";

  // 用户/访客唯一标识符
  public static final String LOG_UUID = "u_ud";

  // 客户端语言
  public static final String LOG_LANG = "l";

  // 会员id 和 业务系统一致
  public static final String LOG_MEMBER_ID = "u_mid";

  // 会话id (session id)
  public static final String LOG_SESSION_ID = "u_sd";

  // 客户端时间
  public static final String LOG_CLIENT_TIME = "c_time";

  // 当前页面的URL
  public static final String LOG_CURRENT_URL = "p_url";

  // 上一个页面的URL
  public static final String LOG_REFER_URL = "p_ref";

  // 当前页面的标题
  public static final String LOG_CURRENT_TITLE = "tt";

  // Event事件的Category名称
  public static final String LOG_CATEGORY = "ca";

  // Event事件的action名称
  public static final String LOG_ACTION = "ac";

  // Event事件的自定义属性
  public static final String LOG_EVENT_PROPERTY = "kv_*";

  // Event事件的持续时间
  public static final String LOG_DURATION = "du";

  // 订单id
  public static final String LOG_ORDER_ID = "oid";

  // 订单名称
  public static final String LOG_ORDER_NAME = "on";

  // 支付金额
  public static final String LOG_CURRENCY_AMOUNT = "cua";

  // 支付货币类型
  public static final String LOG_CURRENCY_TYPE = "cut";

  // 支付方式
  public static final String LOG_PAYMENT_TYPE = "pt";

  
  /** 地区信息 */
  // 国家
  public static final String LOG_REGION_COUNTRY = "country";
  // 省份
  public static final String LOG_REGION_PROVINCE = "province";
  // 城市
  public static final String LOG_REGION_CITY = "city";


  /** 客户端信息 */
  // user-agent
//  public static final String LOG_USER_AGENT = "";;
  // 浏览器信息
  public static final String LOG_BROWSER_NAME = "browser_name";
  // 浏览器版本
  public static final String LOG_BROWSER_VERSION = "browser_version";
  // 操作系统名称
  public static final String LOG_OS_NAME = "os_name";
  // 操作系统版本
  public static final String LOG_OS_VERSION = "os_version";



}
