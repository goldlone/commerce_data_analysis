package cn.goldlone.commerce.dao;

import cn.goldlone.commerce.model.DimensionDate;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

/**
 * @author Created by CN on 2018/12/12/0012 16:17 .
 */
@Mapper
public interface DimDateDao {


  @Select({"select id, year, season, month, week, day, calendar, type ",
          "from dimension_date ",
          "where id = #{id}"})
  DimensionDate selectOneById(int id);
}
