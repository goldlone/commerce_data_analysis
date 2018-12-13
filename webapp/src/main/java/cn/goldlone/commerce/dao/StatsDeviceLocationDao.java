package cn.goldlone.commerce.dao;

import cn.goldlone.commerce.model.StatsDeviceLocation;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author Created by CN on 2018/12/12/0012 23:22 .
 */
@Mapper
public interface StatsDeviceLocationDao {

  String SELECT_FIELDS = "location_dimension_id as locationId, " +
          "sum(active_users) as activeUsers, " +
          "sum(sessions) as sessions, " +
          "sum(bounce_sessions) as bounceSessions";

  @Select({"select ", SELECT_FIELDS,
          " from stats_device_location",
          " group by location_dimension_id",
          " order by activeUsers desc"})
  List<StatsDeviceLocation> selectAll();


}
