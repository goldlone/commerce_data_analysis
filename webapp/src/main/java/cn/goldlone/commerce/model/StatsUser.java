package cn.goldlone.commerce.model;

import java.util.Date;

/**
 * @author Created by CN on 2018/12/12/0012 14:51 .
 */
public class StatsUser {

  private Integer dateId;
  private Integer platformId;
  private Integer activeUsers;
  private Integer newInstallUsers;
  private Integer totalInstallUsers;
  private Integer sessions;
  private Integer sessionsLength;
  private Integer totalMembers;
  private Integer activeMembers;
  private Integer newMembers;
  private Date created;

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

  public Integer getActiveUsers() {
    return activeUsers;
  }

  public void setActiveUsers(Integer activeUsers) {
    this.activeUsers = activeUsers;
  }

  public Integer getNewInstallUsers() {
    return newInstallUsers;
  }

  public void setNewInstallUsers(Integer newInstallUsers) {
    this.newInstallUsers = newInstallUsers;
  }

  public Integer getTotalInstallUsers() {
    return totalInstallUsers;
  }

  public void setTotalInstallUsers(Integer totalInstallUsers) {
    this.totalInstallUsers = totalInstallUsers;
  }

  public Integer getSessions() {
    return sessions;
  }

  public void setSessions(Integer sessions) {
    this.sessions = sessions;
  }

  public Integer getSessionsLength() {
    return sessionsLength;
  }

  public void setSessionsLength(Integer sessionsLength) {
    this.sessionsLength = sessionsLength;
  }

  public Integer getTotalMembers() {
    return totalMembers;
  }

  public void setTotalMembers(Integer totalMembers) {
    this.totalMembers = totalMembers;
  }

  public Integer getActiveMembers() {
    return activeMembers;
  }

  public void setActiveMembers(Integer activeMembers) {
    this.activeMembers = activeMembers;
  }

  public Integer getNewMembers() {
    return newMembers;
  }

  public void setNewMembers(Integer newMembers) {
    this.newMembers = newMembers;
  }

  @Override
  public String toString() {
    return "StatsUser{" +
            "dateId=" + dateId +
            ", platformId=" + platformId +
            ", activeUsers=" + activeUsers +
            ", newInstallUsers=" + newInstallUsers +
            ", totalInstallUsers=" + totalInstallUsers +
            ", sessions=" + sessions +
            ", sessionsLength=" + sessionsLength +
            ", totalMembers=" + totalMembers +
            ", activeMembers=" + activeMembers +
            ", newMembers=" + newMembers +
            '}';
  }
}
