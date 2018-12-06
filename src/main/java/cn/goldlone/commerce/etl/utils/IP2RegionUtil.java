package cn.goldlone.commerce.etl.utils;

import cn.goldlone.commerce.etl.common.EventLogConstants;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Subdivision;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.regex.Pattern;

/**
 * IP地址解析工具类
 * @author Created by CN on 2018/12/2/0002 22:53 .
 */
public class IP2RegionUtil {

  private final static Logger logger = Logger.getLogger(IP2RegionUtil.class);

  public static void main(String[] args) {
//    IP2RegionUtil.getAddress("192.168.1.1");


    System.out.println(IP2RegionUtil.parseIP("123.125.115.110"));
    System.out.println(IP2RegionUtil.parseIP("192.168.1.1"));
    System.out.println(IP2RegionUtil.parseIP("149.28.13.190"));
    System.out.println(IP2RegionUtil.parseIP("39.105.186.70"));
    System.out.println(IP2RegionUtil.parseIP("123.207.159.214"));

//    System.out.println(IP2RegionUtil.getAddress("123.125.115.110"));
//    System.out.println(IP2RegionUtil.getAddress("149.28.13.190"));
//    System.out.println(IP2RegionUtil.getAddress("39.105.186.70"));
//    System.out.println(IP2RegionUtil.getAddress("123.207.159.214"));

//    String pattern = "(172).(16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31).[0-9]*.[0-9]*";
//    System.out.println(Pattern.matches(pattern, "172.31.255.255"));
//    System.out.println(Pattern.matches(pattern, "172.32.0.0"));


  }

//  private static File database = null;
  private static InputStream database = null;
  private static DatabaseReader reader = null;

  private static FileSystem fileSystem = null;

  static {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://hh:9000");
    try {
      fileSystem = FileSystem.get(conf);
//      fileSystem.
//      database = new File(IP2RegionUtil.class.getResource("GeoLite2-City.mmdb").getFile());
      database = fileSystem.open(new Path("/data/GeoLite2-City.mmdb")).getWrappedStream();
      reader = new DatabaseReader.Builder(database).build();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public static RegionInfo parseIP(String ip) {
    RegionInfo info = new RegionInfo();

    String[] patterns = {"(10).([0-9]*).([0-9]*).([0-9]*)",
            "(172).(16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31).[0-9]*.[0-9]*",
            "192.168.([0-9]*).([0-9]*)"};

    for(String pattern : patterns) {
      if(Pattern.matches(pattern, ip)) {
        info.setCountry("局域网");
        return info;
      }
    }

    try {
      InetAddress ipAddress = InetAddress.getByName(ip);

      CityResponse response = reader.city(ipAddress);


      Country country = response.getCountry();
      String countryName = country.getNames().get("zh-CN");

      Subdivision subdivision = response.getMostSpecificSubdivision();
      String subdivisionName = subdivision.getNames().get("zh-CN");

      City city = response.getCity();
      String cityName = city.getNames().get("zh-CN");

      info = new RegionInfo();
      if(StringUtils.isNotBlank(countryName)) {
        info.setCountry(countryName);
      }
      if(StringUtils.isNotBlank(subdivisionName)) {
        info.setProvince(subdivisionName);
      }
      if(StringUtils.isNotBlank(cityName)) {
        info.setCity(cityName);
      }
    } catch (Exception e) {
      logger.warn("【IP解析错误】" + e.getMessage());
    }

    return info;
  }


  public static String getAddress(String ip) {
    try {
      InetAddress ipAddress = InetAddress.getByName(ip);

      CityResponse response = reader.city(ipAddress);


      Country country = response.getCountry();
      String countryName = country.getNames().get("zh-CN");

      Subdivision subdivision = response.getMostSpecificSubdivision();
      String subdivisionName = subdivision.getNames().get("zh-CN");

      City city = response.getCity();
      String cityName = city.getNames().get("zh-CN");

      StringBuilder builder = new StringBuilder();
      if(!StringUtils.isBlank(countryName)) {
        builder.append(countryName);
      }
      if(!StringUtils.isBlank(subdivisionName)) {
        builder.append(subdivisionName);
      }
      if(!StringUtils.isBlank(cityName)) {
        builder.append(cityName);
      }

      return builder.toString();
    } catch (GeoIp2Exception | IOException e) {
      System.out.println("【解析错误】" + e.getMessage());
    }

    return null;
  }

  public static class RegionInfo {

    private String country;

    private String province;

    private String city;

    public RegionInfo() {
      this.country = EventLogConstants.DEFAULT_VALUE;
      this.province = EventLogConstants.DEFAULT_VALUE;
      this.city = EventLogConstants.DEFAULT_VALUE;
    }


    public String getCountry() {
      return country;
    }

    public void setCountry(String country) {
      this.country = country;
    }

    public String getProvince() {
      return province;
    }

    public void setProvince(String province) {
      this.province = province;
    }

    public String getCity() {
      return city;
    }

    public void setCity(String city) {
      this.city = city;
    }

    @Override
    public String toString() {
      return "RegionInfo{" +
              "country='" + country + '\'' +
              ", province='" + province + '\'' +
              ", city='" + city + '\'' +
              '}';
    }
  }

}
