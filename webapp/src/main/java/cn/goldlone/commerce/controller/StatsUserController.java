package cn.goldlone.commerce.controller;

import cn.goldlone.commerce.model.DimensionDate;
import cn.goldlone.commerce.model.Result;
import cn.goldlone.commerce.model.StatsUser;
import cn.goldlone.commerce.service.DimDateService;
import cn.goldlone.commerce.service.DimPlatformService;
import cn.goldlone.commerce.service.StatsUserService;
import cn.goldlone.commerce.utils.ResultUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

/**
 * @author Created by CN on 2018/12/12/0012 15:40 .
 */
@RestController
@RequestMapping("/statsUser")
public class StatsUserController {


  @Autowired
  private StatsUserService statsUserService;

  @Autowired
  private DimPlatformService dimPlatformService;

  @Autowired
  private DimDateService dimDateService;



  @PostMapping("/selectAll")
  public Result selectAll() {

    List<StatsUser> statsUsers = statsUserService.selectAll();


    Map<Integer, Map<String, List<Object>>> map = new HashMap<>();

    for(StatsUser user : statsUsers) {
      int platformId = user.getPlatformId();
      if(!map.containsKey(platformId)) {
        Map<String, List<Object>> m1 = new HashMap<>();
        m1.put("date", new LinkedList<>()); // 时间
        m1.put("active_users", new LinkedList<>());// 活跃用户
        m1.put("new_install_users", new LinkedList<>());// 新增用户
        m1.put("total_install_users", new LinkedList<>());// 总用户
        m1.put("sessions", new LinkedList<>());// 活跃会员
        m1.put("sessions_length", new LinkedList<>());// 新增会员
        m1.put("total_members", new LinkedList<>());// 总会员
        m1.put("active_members", new LinkedList<>());// Session
        m1.put("new_members", new LinkedList<>());// Session时长

        map.put(platformId, m1);
      }

      DimensionDate date = dimDateService.selectOneById(user.getDateId());
      String dateStr = String.valueOf(date.getYear() + "-" + date.getMonth() + "-" + date.getDay());
      map.get(platformId).get("date").add(dateStr);
      map.get(platformId).get("active_users").add(user.getActiveUsers());
      map.get(platformId).get("new_install_users").add(user.getNewInstallUsers());
      map.get(platformId).get("total_install_users").add(user.getTotalInstallUsers());
      map.get(platformId).get("sessions").add(user.getSessions());
      map.get(platformId).get("sessions_length").add(user.getSessionsLength());
      map.get(platformId).get("total_members").add(user.getTotalMembers());
      map.get(platformId).get("active_members").add(user.getActiveMembers());
      map.get(platformId).get("new_members").add(user.getNewMembers());
    }


    Map<String, Map<String, List<Object>>> retMap = new HashMap<>();
    for(int platformId: map.keySet()) {
      retMap.put(dimPlatformService.selectOneById(platformId).getName(), map.get(platformId));
    }


//    Map<Integer, Map<Integer, LinkedList<StatsUser>>> map = new HashMap<Integer, Map<Integer, LinkedList<StatsUser>>>();
//    for(StatsUser user : statsUsers) {
//      int platformId = user.getPlatformId();
//      if (map.containsKey(platformId)) {
//        int dateId = user.getDateId();
//        Map<Integer, LinkedList<StatsUser>> m = map.get(platformId);
//        if (!m.containsKey(dateId)) {
//          m.put(dateId, new LinkedList<StatsUser>());
//        }
//
//        m.get(dateId).add(user);
//      } else {
//        Map<Integer, LinkedList<StatsUser>> m2 = new HashMap<Integer, LinkedList<StatsUser>>();
//        LinkedList<StatsUser> l = new LinkedList<StatsUser>();
//        l.add(user);
//        m2.put(user.getDateId(), l);
//
//        map.put(platformId, m2);
//      }
//    }
//
//
//    Map<String, Map<String, LinkedList<StatsUser>>> map2 = new HashMap<String, Map<String, LinkedList<StatsUser>>>();
//
//    for(int k1 : map.keySet()) {
//
//      String platformName = dimPlatformService.selectOneById(k1).getName();
//      map2.put(platformName, new HashMap<String, LinkedList<StatsUser>>());
//
//      for(int k2 : map.get(k1).keySet()) {
//        DimensionDate date = dimDateService.selectOneById(k2);
//        String dateStr = String.valueOf(date.getYear() + "-" + date.getMonth() + "-" + date.getDay());
//        map2.get(platformName).put(dateStr, map.get(k1).get(k2));
//      }
//    }

    return ResultUtil.success("ok", retMap);
  }




}
