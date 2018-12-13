package cn.goldlone.commerce.service;

import cn.goldlone.commerce.dao.DimKpiDao;
import cn.goldlone.commerce.model.DimensionKpi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author Created by CN on 2018/12/13/0013 14:44 .
 */
@Service
public class DimKpiService {

  @Autowired
  private DimKpiDao dimKpiDao;


  public DimensionKpi selectOneById(int id) {

    return dimKpiDao.selectOne(id);
  }


  public int selectIdByName(String name) {

    return dimKpiDao.selectIdByName(name);
  }

}
