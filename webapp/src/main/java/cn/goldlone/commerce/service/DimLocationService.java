package cn.goldlone.commerce.service;

import cn.goldlone.commerce.dao.DimLocationDao;
import cn.goldlone.commerce.model.DimensionLocation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author Created by CN on 2018/12/13/0013 8:51 .
 */
@Service
public class DimLocationService {

  @Autowired
  private DimLocationDao dimLocationDao;


  public DimensionLocation selectOneById(int id) {

    return dimLocationDao.selectOneById(id);
  }

}
