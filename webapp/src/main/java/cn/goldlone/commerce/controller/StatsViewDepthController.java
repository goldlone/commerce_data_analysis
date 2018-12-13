package cn.goldlone.commerce.controller;

import cn.goldlone.commerce.model.DimensionDate;
import cn.goldlone.commerce.model.Result;
import cn.goldlone.commerce.model.StatsViewDepth;
import cn.goldlone.commerce.service.DimDateService;
import cn.goldlone.commerce.service.DimKpiService;
import cn.goldlone.commerce.service.DimPlatformService;
import cn.goldlone.commerce.service.StatsViewDepthService;
import cn.goldlone.commerce.utils.ResultUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Created by CN on 2018/12/13/0013 12:02 .
 */
@RestController
@RequestMapping("/viewDepth")
public class StatsViewDepthController {

  @Autowired
  private StatsViewDepthService statsViewDepthService;

  @Autowired
  private DimPlatformService dimPlatformService;

  @Autowired
  private DimDateService dimDateService;

  @Autowired
  private DimKpiService dimKpiService;

  @PostMapping("/selectAll")
  public Result selectAll() {

    List<StatsViewDepth> statsViewDepths = statsViewDepthService.selectAll();

    System.out.println(statsViewDepths);

    // 只取活跃用户指标
    int kpiId = dimKpiService.selectIdByName("activity_user");

    Map<Integer, Map<String, List<Object>>> map = new HashMap<>();
    for(StatsViewDepth viewDepth : statsViewDepths) {

      if(viewDepth.getKpiId() != kpiId) {
        continue;
      }

      int platformId = viewDepth.getPlatformId();
      if(!map.containsKey(platformId)) {
        Map<String, List<Object>> m1 = new HashMap<>();
        m1.put("date", new LinkedList<>()); // 日期
        m1.put("pv1", new LinkedList<>());//
        m1.put("pv2", new LinkedList<>());//
        m1.put("pv3", new LinkedList<>());//
        m1.put("pv4", new LinkedList<>());//
        m1.put("pv5_10", new LinkedList<>());//
        m1.put("pv10_30", new LinkedList<>());//
        m1.put("pv30_60", new LinkedList<>());//
        m1.put("pv60_plus", new LinkedList<>());//

        map.put(platformId, m1);
      }

      DimensionDate date = dimDateService.selectOneById(viewDepth.getDateId());
      String dateStr = String.valueOf(date.getYear() + "-" + date.getMonth() + "-" + date.getDay());
      map.get(platformId).get("date").add(dateStr);
      map.get(platformId).get("pv1").add(viewDepth.getPv1());
      map.get(platformId).get("pv2").add(viewDepth.getPv2());
      map.get(platformId).get("pv3").add(viewDepth.getPv3());
      map.get(platformId).get("pv4").add(viewDepth.getPv4());
      map.get(platformId).get("pv5_10").add(viewDepth.getPv5to10());
      map.get(platformId).get("pv10_30").add(viewDepth.getPv10to30());
      map.get(platformId).get("pv30_60").add(viewDepth.getPv30to60());
      map.get(platformId).get("pv60_plus").add(viewDepth.getPv60plus());
    }

    Map<String, Map<String, List<Object>>> retMap = new HashMap<>();
    for(int platformId: map.keySet()) {
      retMap.put(dimPlatformService.selectOneById(platformId).getName(), map.get(platformId));
    }


    return ResultUtil.success("ok", retMap);
  }


}
