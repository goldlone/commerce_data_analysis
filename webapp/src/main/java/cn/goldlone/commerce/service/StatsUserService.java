package cn.goldlone.commerce.service;

import cn.goldlone.commerce.dao.StatsUserDao;
import cn.goldlone.commerce.model.Result;
import cn.goldlone.commerce.model.StatsUser;
import cn.goldlone.commerce.utils.ResultUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Created by CN on 2018/12/12/0012 15:41 .
 */
@Service
public class StatsUserService {

  @Autowired
  private StatsUserDao statsUserDao;



  public List<StatsUser> selectAll() {

    return statsUserDao.selectAll();
  }





}
