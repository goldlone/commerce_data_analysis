package cn.goldlone.commerce.model

/**
  *
  * @author Created by CN on 2018/12/5/0005 11:34 .
  */
class DimensionLocation extends BaseDimension {
  
  var id: Int = _
  
  var country: String = _
  
  var province: String = _
  
  var city: String = _
  
  def this(country: String, province: String, city: String) {
    this()
    this.country = country
    this.province = province
    this.city = city
  }
  
  
  
}
