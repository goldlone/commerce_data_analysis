package cn.goldlone.commerce.dao;

import cn.goldlone.commerce.model.StatsViewDepth;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author Created by CN on 2018/12/13/0013 12:01 .
 */
@Mapper
public interface StatsViewDepthDao {

  String SELECT_FIELDS = "platform_dimension_id as platformId, " +
          "date_dimension_id as dateId, " +
          "kpi_dimension_id as kpiId, " +
          "pv1, pv2, " +
          "pv3, pv4, " +
          "pv5_10 as pv5to10, pv10_30 as pv10to30, " +
          "pv30_60 as pv30to60, " +
          "`pv60+` as pv60plus";

  @Select({"select ", SELECT_FIELDS, " from stats_view_depth"})
  List<StatsViewDepth> selectAll();


}
