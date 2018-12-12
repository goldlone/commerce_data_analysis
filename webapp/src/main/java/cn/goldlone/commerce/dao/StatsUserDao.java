package cn.goldlone.commerce.dao;

import cn.goldlone.commerce.model.StatsUser;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author Created by CN on 2018/12/12/0012 15:41 .
 */
@Mapper
public interface StatsUserDao {

  String SELECT_FIELD = "date_dimension_id as dateId, " +
          "platform_dimension_id as platformId, " +
          "active_users as activeUsers, " +
          "new_install_users as newInstallUsers, " +
          "total_install_users as totalInstallUsers, " +
          "sessions, " +
          "sessions_length as sessionsLength, " +
          "total_members as totalMembers, " +
          "active_members as activeMembers, " +
          "new_members as newMembers ";

  String TABLE = "stats_user";


  @Select({"select ", SELECT_FIELD, " from ", TABLE})
  List<StatsUser> selectAll();



}
