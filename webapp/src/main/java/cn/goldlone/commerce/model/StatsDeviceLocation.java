package cn.goldlone.commerce.model;

/**
 * @author Created by CN on 2018/12/12/0012 14:57 .
 */
public class StatsDeviceLocation {
  private Integer date_dimension_id;

  private Integer platform_dimension_id;

  private Integer location_dimension_id;

  private Integer active_users;

  private Integer sessions;

  private Integer bounce_sessions;

  private Integer created;

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

  public Integer getLocation_dimension_id() {
    return location_dimension_id;
  }

  public void setLocation_dimension_id(Integer location_dimension_id) {
    this.location_dimension_id = location_dimension_id;
  }

  public Integer getActive_users() {
    return active_users;
  }

  public void setActive_users(Integer active_users) {
    this.active_users = active_users;
  }

  public Integer getSessions() {
    return sessions;
  }

  public void setSessions(Integer sessions) {
    this.sessions = sessions;
  }

  public Integer getBounce_sessions() {
    return bounce_sessions;
  }

  public void setBounce_sessions(Integer bounce_sessions) {
    this.bounce_sessions = bounce_sessions;
  }

  public Integer getCreated() {
    return created;
  }

  public void setCreated(Integer created) {
    this.created = created;
  }
}
