package cn.goldlone.commerce.dao;

import cn.goldlone.commerce.model.DimensionPlatform;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

/**
 * @author Created by CN on 2018/12/12/0012 15:57 .
 */
@Mapper
public interface DimPlatformDao {


  @Select({"select id, platform_name as name ",
          "from dimension_platform ",
          "where id = #{id}"})
  DimensionPlatform selectOneById(int id);


}
