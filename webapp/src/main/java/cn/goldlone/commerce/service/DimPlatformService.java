package cn.goldlone.commerce.service;

import cn.goldlone.commerce.dao.DimPlatformDao;
import cn.goldlone.commerce.model.DimensionPlatform;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author Created by CN on 2018/12/12/0012 15:57 .
 */
@Service
public class DimPlatformService {


  @Autowired
  private DimPlatformDao dimPlatformDao;


  public DimensionPlatform selectOneById(int id) {

    return dimPlatformDao.selectOneById(id);
  }

}
