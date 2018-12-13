package cn.goldlone.commerce.service;

import cn.goldlone.commerce.dao.StatsViewDepthDao;
import cn.goldlone.commerce.model.StatsViewDepth;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Created by CN on 2018/12/13/0013 12:02 .
 */
@Service
public class StatsViewDepthService {

  @Autowired
  private StatsViewDepthDao statsViewDepthDao;

  public List<StatsViewDepth> selectAll() {

    return statsViewDepthDao.selectAll();
  }

}
