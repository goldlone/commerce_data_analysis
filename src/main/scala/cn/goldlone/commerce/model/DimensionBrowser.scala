package cn.goldlone.commerce.model

/**
  *
  * @author Created by CN on 2018/12/5/0005 11:30 .
  */
class DimensionBrowser extends BaseDimension {

  var id: Int = _
  
  var name: String = _
  
  var version: String = _
  
  def this(name: String, version: String) {
    this()
    this.name = name
    this.version = version
  }
  
}
