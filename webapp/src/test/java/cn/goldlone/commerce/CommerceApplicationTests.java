package cn.goldlone.commerce;

import cn.goldlone.commerce.dao.StatsHourlyDao;
import cn.goldlone.commerce.model.StatsHourly;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class CommerceApplicationTests {

  @Test
  public void contextLoads() {
  }


  @Autowired
  private StatsHourlyDao statsHourlyDao;

  @Test
  public void test() {
    List<StatsHourly> statsHourlies = statsHourlyDao.selectAll();


    System.out.println(statsHourlies);
  }
}
