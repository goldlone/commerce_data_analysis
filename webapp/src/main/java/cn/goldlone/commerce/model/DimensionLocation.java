package cn.goldlone.commerce.model;

/**
 * @author Created by CN on 2018/12/12/0012 14:45 .
 */
public class DimensionLocation {

  private Integer id;


  private String country;


  private String province;

  private String city;


  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
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
}
