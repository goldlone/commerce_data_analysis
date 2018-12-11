package cn.goldlone.commerce.udfs

import cn.goldlone.commerce.dao.DimensionDao
import cn.goldlone.commerce.model.{DimensionDate, DimensionKpi}
import org.apache.hadoop.hive.ql.exec.UDF

/**
  *
  * @author Created by CN on 2018/12/11/0011 17:54 .
  */
class KpiUdf extends UDF {
  
  private val dimensionDao = new DimensionDao()
  
  def evaluate(s: String): Int = {
    
    val kpi = new DimensionKpi(s)
    
    dimensionDao.getDimensionIdByValue(kpi)
    
  }
}
