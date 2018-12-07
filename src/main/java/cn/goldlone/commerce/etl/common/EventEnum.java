package cn.goldlone.commerce.etl.common;

/**
 * @author Created by CN on 2018/12/4/0004 11:35 .
 */
public enum EventEnum {

  LAUNCH(1, "launch event", "e_l"),
  PAGE_VIEW(2, "page view event", "e_pv"),
  CHARGE_REQUEST(3, "charge request event", "e_crt"),
  CHARGE_SUCCESS(4, "charge success event", "e_cs"),
  CHARGE_REFUSE(5, "charge event refuse", "e_cr"),
  EVENT(6, "event", "e");

  private int value;

  private String name;

  private String alias;

  EventEnum(int value, String name, String alias) {
    this.value = value;
    this.name = name;
    this.alias = alias;
  }

  public String getAlias() {
    return this.alias;
  }


  public static EventEnum valueOfAlias(String aliasName) {

    for(EventEnum eventEnum : values()) {
      if(eventEnum.alias.equals(aliasName)) {
        return eventEnum;
      }
    }

    return null;
  }

}
