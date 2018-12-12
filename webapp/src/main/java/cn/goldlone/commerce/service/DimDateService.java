package cn.goldlone.commerce.service;

import cn.goldlone.commerce.dao.DimDateDao;
import cn.goldlone.commerce.model.DimensionDate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author Created by CN on 2018/12/12/0012 16:16 .
 */
@Service
public class DimDateService {


  @Autowired
  private DimDateDao dimDateDao;


  public DimensionDate selectOneById(int id) {

    return dimDateDao.selectOneById(id);
  }

}
