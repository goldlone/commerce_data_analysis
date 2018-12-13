package cn.goldlone.commerce.dao;

import cn.goldlone.commerce.model.DimensionLocation;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

/**
 * @author Created by CN on 2018/12/13/0013 8:48 .
 */
@Mapper
public interface DimLocationDao {


  @Select({"select id, country, province, city " +
          "from dimension_location " +
          "where id=#{id}"})
  DimensionLocation selectOneById(int id);


}
