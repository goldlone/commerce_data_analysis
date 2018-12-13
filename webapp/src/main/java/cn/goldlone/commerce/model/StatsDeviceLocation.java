package cn.goldlone.commerce.model;

/**
 * @author Created by CN on 2018/12/12/0012 14:57 .
 */
public class StatsDeviceLocation {

  private Integer dateId;

  private Integer platformId;

  private Integer locationId;

  private Integer activeUsers;

  private Integer sessions;

  private Integer bounceSessions;

  public Integer getDateId() {
    return dateId;
  }

  public void setDateId(Integer dateId) {
    this.dateId = dateId;
  }

  public Integer getPlatformId() {
    return platformId;
  }

  public void setPlatformId(Integer platformId) {
    this.platformId = platformId;
  }

  public Integer getLocationId() {
    return locationId;
  }

  public void setLocationId(Integer locationId) {
    this.locationId = locationId;
  }

  public Integer getActiveUsers() {
    return activeUsers;
  }

  public void setActiveUsers(Integer activeUsers) {
    this.activeUsers = activeUsers;
  }

  public Integer getSessions() {
    return sessions;
  }

  public void setSessions(Integer sessions) {
    this.sessions = sessions;
  }

  public Integer getBounceSessions() {
    return bounceSessions;
  }

  public void setBounceSessions(Integer bounceSessions) {
    this.bounceSessions = bounceSessions;
  }

  @Override
  public String toString() {
    return "StatsDeviceLocation{" +
            "dateId=" + dateId +
            ", platformId=" + platformId +
            ", locationId=" + locationId +
            ", activeUsers=" + activeUsers +
            ", sessions=" + sessions +
            ", bounceSessions=" + bounceSessions +
            '}';
  }
}
