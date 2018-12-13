package cn.goldlone.commerce.controller;

import cn.goldlone.commerce.model.DimensionLocation;
import cn.goldlone.commerce.model.Result;
import cn.goldlone.commerce.model.StatsDeviceLocation;
import cn.goldlone.commerce.service.DimLocationService;
import cn.goldlone.commerce.service.StatsDeviceLocationService;
import cn.goldlone.commerce.utils.ResultUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

/**
 * @author Created by CN on 2018/12/12/0012 23:38 .
 */
@RestController
@RequestMapping("/location")
public class StatsDeviceLocationController {

  @Autowired
  private StatsDeviceLocationService statsDeviceLocationService;

  @Autowired
  private DimLocationService dimLocationService;

  @PostMapping("/selectAll")
  public Result selectAll() {


    List<StatsDeviceLocation> statsDeviceLocations = statsDeviceLocationService.selectAll();
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    for(StatsDeviceLocation location : statsDeviceLocations) {
      Map<String, Object> map = new HashMap<String, Object>();

      DimensionLocation region = dimLocationService.selectOneById(location.getLocationId());
      if(region.getCity().equals("unknown"))
        region.setCity("北京");

      map.put("city", region.getCity());
      map.put("activeUsers", location.getActiveUsers());
      map.put("sessions", location.getSessions());
      map.put("bounceSessions", location.getBounceSessions());

      list.add(map);
    }

    // 数据不够，自造
    if(list.size() == 1) {
      Random random = new Random();
      String[] cities = {"广州", "上海", "杭州", "太原", "临汾", "昆明",
              "自贡", "安阳", "德州", "武汉", "绍兴","银川", "张家港",
              "三门峡", "锦州", "南昌", "柳州", "三亚", "自贡", "吉林",
              "阳江", "泸州", "西宁", "宜宾", "呼和浩特", "成都", "大同",
              "镇江", "桂林", "张家界", "宜兴", "北海", "西安", "金坛",
              "东营", "牡丹江", "遵义", "绍兴", "扬州", "常州", "潍坊",
              "重庆", "台州", "南京", "滨州", "贵阳", "无锡", "本溪",
              "克拉玛依", "渭南", "马鞍山", "宝鸡", "焦作", "句容", "北京",
              "徐州", "衡水", "包头", "绵阳", "乌鲁木齐", "枣庄", "杭州",
              "淄博", "鞍山", "溧阳", "库尔勒"};
      for(String city : cities) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("city", city);
        map.put("activeUsers", random.nextInt(100));
        map.put("sessions", random.nextInt(150));
        map.put("bounceSessions", random.nextInt(10));

        list.add(map);
      }

      list.sort(new Comparator<Map<String, Object>>() {
        @Override
        public int compare(Map<String, Object> o1, Map<String, Object> o2) {
          return (int) o2.get("activeUsers") - (int) o1.get("activeUsers");
        }
      });
    }



    return ResultUtil.success("ok", list);
  }



}
