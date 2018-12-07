package cn.goldlone.commerce.model

/**
  *
  * @author Created by CN on 2018/12/5/0005 11:16 .
  */
class DimensionKpi extends BaseDimension {

  var id: Int = _

  var name: String = _
  
  def this(id: Int, name: String) {
    this()
    this.id = id
    this.name = name
  }
  
  def this(name: String) {
    this()
    this.name = name
  }
  
}
