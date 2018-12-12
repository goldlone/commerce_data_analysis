package cn.goldlone.commerce.model;

import java.util.Date;

/**
 * @author Created by CN on 2018/12/12/0012 14:56 .
 */
public class StatsDeviceBrowser {

  private Integer date_dimension_id;
  private Integer platform_dimension_id;
  private Integer browser_dimension_id;

  private Integer active_users;
  private Integer new_install_users;
  private Integer total_install_users;
  private Integer sessions;
  private Integer sessions_length;
  private Integer total_members;
  private Integer active_members;
  private Integer new_members;
  private Date created;


  public Integer getDate_dimension_id() {
    return date_dimension_id;
  }

  public void setDate_dimension_id(Integer date_dimension_id) {
    this.date_dimension_id = date_dimension_id;
  }

  public Integer getPlatform_dimension_id() {
    return platform_dimension_id;
  }

  public void setPlatform_dimension_id(Integer platform_dimension_id) {
    this.platform_dimension_id = platform_dimension_id;
  }

  public Integer getBrowser_dimension_id() {
    return browser_dimension_id;
  }

  public void setBrowser_dimension_id(Integer browser_dimension_id) {
    this.browser_dimension_id = browser_dimension_id;
  }

  public Integer getActive_users() {
    return active_users;
  }

  public void setActive_users(Integer active_users) {
    this.active_users = active_users;
  }

  public Integer getNew_install_users() {
    return new_install_users;
  }

  public void setNew_install_users(Integer new_install_users) {
    this.new_install_users = new_install_users;
  }

  public Integer getTotal_install_users() {
    return total_install_users;
  }

  public void setTotal_install_users(Integer total_install_users) {
    this.total_install_users = total_install_users;
  }

  public Integer getSessions() {
    return sessions;
  }

  public void setSessions(Integer sessions) {
    this.sessions = sessions;
  }

  public Integer getSessions_length() {
    return sessions_length;
  }

  public void setSessions_length(Integer sessions_length) {
    this.sessions_length = sessions_length;
  }

  public Integer getTotal_members() {
    return total_members;
  }

  public void setTotal_members(Integer total_members) {
    this.total_members = total_members;
  }

  public Integer getActive_members() {
    return active_members;
  }

  public void setActive_members(Integer active_members) {
    this.active_members = active_members;
  }

  public Integer getNew_members() {
    return new_members;
  }

  public void setNew_members(Integer new_members) {
    this.new_members = new_members;
  }

  public Date getCreated() {
    return created;
  }

  public void setCreated(Date created) {
    this.created = created;
  }
}
