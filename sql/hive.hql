
# 创建数据库
create database commerce;

# 创建访问数据表
drop table if exists access_log;
create table if not exists access_log(
  ip string,
  country string,
  province string,
  city string,
  time string,
  request_method string,
  request_protocol string,
  request_url string,
  status string,
  http_referer string,
  os_name string,
  os_version string,
  browser_name string,
  browser_version string)
row format delimited
fields terminated by '\t';




