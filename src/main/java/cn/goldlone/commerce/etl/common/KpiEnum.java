package cn.goldlone.commerce.etl.common;

/**
 * KPI 指标枚举类
 * @author Created by CN on 2018/12/6/0006 21:13 .
 */
public enum KpiEnum {

  /** 活跃用户 */
  ACTIVITY_USER("activity_user"),

  /** 新增用户 */
  NEW_INSTALL_USER("new_install_user"),

  /** 总用户 */
  TOTAL_INSTALL_USER("total_install_user"),

  /** 活跃会员 */
  ACTIVITY_MEMBER("activity_member"),

  /** 新增会员 */
  NEW_INSTALL_MEMBER("new_install_member"),

  /** 总用户 */
  TOTAL_INSTALL_MEMBER("total_install_member"),

  /** 会话个数 */
  SESSION_COUNT("session_count"),

  /** 会话长度 */
  SESSION_LENGTH("session_length");


  private String name;


  private KpiEnum(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

}
