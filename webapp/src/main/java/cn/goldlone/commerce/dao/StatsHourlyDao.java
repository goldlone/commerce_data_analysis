package cn.goldlone.commerce.dao;

import cn.goldlone.commerce.model.StatsHourly;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author Created by CN on 2018/12/12/0012 19:32 .
 */
@Mapper
public interface StatsHourlyDao {

  String SELECT_FIELDS = "platform_dimension_id as platformdimensionid, " +
          "date_dimension_id as datedimensionid, " +
          "kpi_dimension_id as kpidimensionid, " +
          "hour_00 as hour00, " +
          "hour_01 as hour01, " +
          "hour_02 as hour02, " +
          "hour_03 as hour03, " +
          "hour_04 as hour04, " +
          "hour_05 as hour05, " +
          "hour_06 as hour06, " +
          "hour_07 as hour07, " +
          "hour_08 as hour08, " +
          "hour_09 as hour09, " +
          "hour_10 as hour10, " +
          "hour_11 as hour11, " +
          "hour_12 as hour12, " +
          "hour_13 as hour13, " +
          "hour_14 as hour14, " +
          "hour_15 as hour15, " +
          "hour_16 as hour16, " +
          "hour_17 as hour17, " +
          "hour_18 as hour18, " +
          "hour_19 as hour19, " +
          "hour_20 as hour20, " +
          "hour_21 as hour21, " +
          "hour_22 as hour22, " +
          "hour_23 as hour23 ";

  @Select({"select ", SELECT_FIELDS," from stats_hourly"})
  List<StatsHourly> selectAll();

}
