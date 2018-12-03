package cn.goldlone.commerce.etl.utils;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Created by CN on 2018/12/3/0003 19:31 .
 */
public class UserAgentUtil {

  private static final Logger logger = Logger.getLogger(UserAgentUtil.class);

  private static UASparser uaSparser = null;


//  public static void main(String[] args) {
//
//    System.out.println(parseUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36"));
//
//    System.out.println(parseUserAgent("Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.221 Safari/537.36 SE 2.X MetaSr 1.0"));
//
//    System.out.println(parseUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134"));
//    System.out.println(parseUserAgent(""));
//  }

  static {
    try {
      uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  public static UserAgentInfo parseUserAgent(String userAgent) {

    UserAgentInfo info = null;

    try {
      if(StringUtils.isBlank(userAgent))
        return null;

      cz.mallat.uasparser.UserAgentInfo uInfo = uaSparser.parse(userAgent);

      if(uInfo != null) {
        info = new UserAgentInfo();
        info.setBrowserName(uInfo.getUaFamily());
        info.setBrowserVersion(uInfo.getBrowserVersionInfo());
        info.setOsName(uInfo.getOsFamily());
        info.setOsVersion(uInfo.getOsName());
      }
    } catch (IOException e) {
      logger.error("userAgent解析失败", e);
    }

    return info;
  }


  public static class UserAgentInfo {

    private String browserName;

    private String browserVersion;

    private String osName;

    private String osVersion;

    public String getBrowserName() {
      return browserName;
    }

    public void setBrowserName(String browserName) {
      this.browserName = browserName;
    }

    public String getBrowserVersion() {
      return browserVersion;
    }

    public void setBrowserVersion(String browserVersion) {
      this.browserVersion = browserVersion;
    }

    public String getOsName() {
      return osName;
    }

    public void setOsName(String osName) {
      this.osName = osName;
    }

    public String getOsVersion() {
      return osVersion;
    }

    public void setOsVersion(String osVersion) {
      this.osVersion = osVersion;
    }

    @Override
    public String toString() {
      return "UserAgentInfo{" +
              "browserName='" + browserName + '\'' +
              ", browserVersion='" + browserVersion + '\'' +
              ", osName='" + osName + '\'' +
              ", osVersion='" + osVersion + '\'' +
              '}';
    }
  }
}
