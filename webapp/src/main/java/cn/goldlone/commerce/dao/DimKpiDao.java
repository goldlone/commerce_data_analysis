package cn.goldlone.commerce.dao;

import cn.goldlone.commerce.model.DimensionKpi;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

/**
 * @author Created by CN on 2018/12/13/0013 14:42 .
 */
@Mapper
public interface DimKpiDao {


  @Select({"select id,kpi_name as name " +
          "from dimension_kpi " +
          "where id=#{id}"})
  DimensionKpi selectOne(int id);


  @Select({"select id " +
          "from dimension_kpi " +
          "where kpi_name=#{name}"})
  int selectIdByName(String name);

}
