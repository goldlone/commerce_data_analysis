package cn.goldlone.commerce.service;

import cn.goldlone.commerce.dao.StatsDeviceLocationDao;
import cn.goldlone.commerce.model.StatsDeviceLocation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Created by CN on 2018/12/12/0012 23:28 .
 */
@Service
public class StatsDeviceLocationService {

  @Autowired
  private StatsDeviceLocationDao statsDeviceLocationDao;

  public List<StatsDeviceLocation> selectAll() {

    return statsDeviceLocationDao.selectAll();
  }


}
