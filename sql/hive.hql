
# 创建数据库
create database commerce;

# 创建日志数据表
drop table if exists access_log;
create table if not exists access_log(
  ip string,
  server_time bigint,
  event string,
  platform string,
  sdk string,
  country string,
  province string,
  city string,
  browser_name string,
  browser_version string,
  os_name string,
  os_version string,
  uuid string,
  lang string,
  member_id string,
  session_id string,
  client_time bigint,
  current_url string,
  refer_url string,
  event_category string,
  event_action string,
  event_duration string,
  event_properties array<string>,
  order_id string,
  order_name string,
  currency_amount string,
  currency_type string,
  payment_type string
) row format delimited
fields terminated by '\u0001'
stored as textfile;

load data inpath '/data/commerce/etl/2018/12/05' into table access_log;


-- 外部分区表
drop table if exists logs;
create external table if not exists logs(
ip string,
server_time bigint,
event string,
platform string,
sdk string,
country string,
province string,
city string,
browser_name string,
browser_version string,
os_name string,
os_version string,
uuid string,
lang string,
member_id string,
session_id string,
client_time bigint,
current_url string,
refer_url string,
event_category string,
event_action string,
event_duration string,
event_properties array<string>,
order_id string,
order_name string,
currency_amount string,
currency_type string,
payment_type string
)
partitioned by(year string, month string, day string)
row format delimited
fields terminated by '\u0001'
stored as textfile;

-- 导入分区数据
alter table log1 add partition(year='2018', month='12', day='05') location 'hdfs://hh:9000/data/commerce/etl/2018/12/05';
alter table log1 add partition(year='2018', month='12', day='06') location 'hdfs://hh:9000/data/commerce/etl/2018/12/06';

-- 1. 用户浏览深度分析 => 根据时间，平台，KPI（用户|Session）分组，统计pageView的个数
-- 1.1 用户维度:
-- 1.1.1 pv数统计
drop table if exists stats_view_depth_time_platform_user;
create table stats_view_depth_time_platform_user
as
select from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd') date,
    platform,
    uuid,
    count(1) pv,
    case when count(1) <= 0 then 'other'
      when count(1)=1 then 'pv1'
      when count(1)=2 then 'pv2'
      when count(1)=3 then 'pv3'
      when count(1)=4 then 'pv4'
      when count(1) <= 10 then 'pv5_10'
      when count(1) <= 30 then 'pv10_30'
      when count(1) <= 60 then 'pv30_60'
      when count(1) > 60 then 'pv60+'
      else 'other' end pv_type
from access_log
where event='e_pv'
group by from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd'), 
    platform,
    uuid
order by date asc;

-- 1.1.2 pv数分组计数（每个PV下对应着多少用户数）
drop table if exists stats_view_depth_time_platform_user_count;
create table stats_view_depth_time_platform_user_count
as
select date,platform,pv_type,count(pv_type) count
from stats_view_depth_time_platform_user
group by date,platform,pv_type;

-- 1.1.3 多行转一行
drop table if exists stats_view_depth_kpi_user;
create table stats_view_depth_kpi_user
as
select date,platform,concat_ws(',', collect_set(concat(pv_type, '=', count))) pvs
from stats_view_depth_time_platform_user_count
group by date,platform;

-- 1.1.3 另一种：多行转多列
drop table if exists stats_view_depth_kpi_user2;
create table stats_view_depth_kpi_user2
as
select date,
    platform,
    concat_ws('', collect_set(if(pv_type = 'pv1', count, '0')), '') pv1,
    concat_ws('', collect_set(if(pv_type = 'pv2', count, '0')), '') pv2,
    concat_ws('', collect_set(if(pv_type = 'pv3', count, '0')), '') pv3,
    concat_ws('', collect_set(if(pv_type = 'pv4', count, '0')), '') pv4,
    concat_ws('', collect_set(if(pv_type = 'pv5_10', count, '0')), '') pv5_10,
    concat_ws('', collect_set(if(pv_type = 'pv10_30', count, '0')), '') pv10_30,
    concat_ws('', collect_set(if(pv_type = 'pv30_60', count, '0')), '') pv30_60,
    concat_ws('', collect_set(if(pv_type = 'pv60+', count, '0')), '') `pv60+`
from stats_view_depth_time_platform_user_count
group by date,platform;

-- 1.1.2 从1直接到2两步即可
set hive.exec.mode.local.auto=true; -- 本地模式
set hive.groupby.skewindata=true; -- 预防数据倾斜，并进行负载均衡，在进行查询时会启动两个job，第一个Job会将Map的输出结果随机的分布到reduce中进行部分聚合，第二个job最终完成全部数据的聚合（group by 到同一个reduce中）
set hive.map.aggr=true; -- 在map端进行聚合
set hive.exec.parallel=true; -- job并行执行
set hive.exec.mode.local.auto.input.files.max=7; -- 本地模式支持最大的文件个数，数量不要太大，由于本地模式在同一JVM中执行任务，如果数据量过大会造成OOM(out of memory)
set hive.exec.mode.local.auto.inputbytes.max = 134217728  -- 128M
drop table if exists stats_view_depth_kpi_user3;
create table stats_view_depth_kpi_user3
as
select date, platform, sum(pv1) as pv1, sum(pv2) as pv2, sum(pv3) as pv3, sum(pv4) as pv4, sum(pv5_10) as pv5_10, sum(pv10_30) as pv10_30, sum(`pv_60+`) as `pv_60+`
from (
  select date, platform, pv as pv1, 0 pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv30_60, 0 as `pv_60+` from stats_view_depth_time_platform_user where pv_type='pv1' union all 
  select date, platform, 0 as pv1, pv pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv30_60, 0 as `pv_60+` from stats_view_depth_time_platform_user where pv_type='pv2' union all 
  select date, platform, 0 as pv1, 0 pv2, pv as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv30_60, 0 as `pv_60+` from stats_view_depth_time_platform_user where pv_type='pv3' union all 
  select date, platform, 0 as pv1, 0 pv2, 0 as pv3, pv as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv30_60, 0 as `pv_60+` from stats_view_depth_time_platform_user where pv_type='pv4' union all 
  select date, platform, 0 as pv1, 0 pv2, 0 as pv3, 0 as pv4, pv as pv5_10, 0 as pv10_30, 0 as pv30_60, 0 as `pv_60+` from stats_view_depth_time_platform_user where pv_type='pv5_10' union all 
  select date, platform, 0 as pv1, 0 pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, pv as pv10_30, 0 as pv30_60, 0 as `pv_60+` from stats_view_depth_time_platform_user where pv_type='pv10_30' union all 
  select date, platform, 0 as pv1, 0 pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, pv as pv30_60, 0 as `pv_60+` from stats_view_depth_time_platform_user where pv_type='pv30_60' union all 
  select date, platform, 0 as pv1, 0 pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv30_60, pv as `pv_60+` from stats_view_depth_time_platform_user where pv_type='pv60+' 
) tmp
group by date,platform;




-- 1.2 会话维度:
-- 1.2.1 pv数统计
drop table if exists stats_view_depth_time_platform_session;
create table stats_view_depth_time_platform_session
as
select from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd') date,
    platform,
    session_id,
    count(1) pv,
    case when count(1) <= 0 then 'other'
      when count(1) = 1 then 'pv1'
      when count(1) = 2 then 'pv2'
      when count(1) = 3 then 'pv3'
      when count(1) = 4 then 'pv4'
      when count(1) <= 10 then 'pv5_10'
      when count(1) <= 30 then 'pv10_30'
      when count(1) <= 60 then 'pv30_60'
      when count(1) > 60 then 'pv60+'
      else 'other' end pv_type
from access_log
where event='e_pv'
group by from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd'), 
    platform,
    session_id
order by date asc;

drop table if exists stats_view_depth_kpi_session3;
create table stats_view_depth_kpi_session3
as
select date, platform, sum(pv1) as pv1, sum(pv2) as pv2, sum(pv3) as pv3, sum(pv4) as pv4, sum(pv5_10) as pv5_10, sum(pv10_30) as pv10_30, sum(`pv_60+`) as `pv_60+`
from (
  select date, platform, pv as pv1, 0 pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv30_60, 0 as `pv_60+` from stats_view_depth_time_platform_session where pv_type='pv1' union all 
  select date, platform, 0 as pv1, pv pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv30_60, 0 as `pv_60+` from stats_view_depth_time_platform_session where pv_type='pv2' union all 
  select date, platform, 0 as pv1, 0 pv2, pv as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv30_60, 0 as `pv_60+` from stats_view_depth_time_platform_session where pv_type='pv3' union all 
  select date, platform, 0 as pv1, 0 pv2, 0 as pv3, pv as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv30_60, 0 as `pv_60+` from stats_view_depth_time_platform_session where pv_type='pv4' union all 
  select date, platform, 0 as pv1, 0 pv2, 0 as pv3, 0 as pv4, pv as pv5_10, 0 as pv10_30, 0 as pv30_60, 0 as `pv_60+` from stats_view_depth_time_platform_session where pv_type='pv5_10' union all 
  select date, platform, 0 as pv1, 0 pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, pv as pv10_30, 0 as pv30_60, 0 as `pv_60+` from stats_view_depth_time_platform_session where pv_type='pv10_30' union all 
  select date, platform, 0 as pv1, 0 pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, pv as pv30_60, 0 as `pv_60+` from stats_view_depth_time_platform_session where pv_type='pv30_60' union all 
  select date, platform, 0 as pv1, 0 pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv30_60, pv as `pv_60+` from stats_view_depth_time_platform_session where pv_type='pv60+' 
) tmp
group by date,platform;

-- 1.2.2 pv数分组计数(每个PV下对应着多少会话数)
drop table if exists stats_view_depth_time_platform_session_count;
create table stats_view_depth_time_platform_session_count
as
select date,platform,pv_type,count(pv_type) count
from stats_view_depth_time_platform_session
group by date,platform,pv_type;

-- 1.2.3 
drop table if exists stats_view_depth_kpi_session;
create table stats_view_depth_kpi_session
as
select date,platform,concat_ws(',', collect_set(concat(pv_type, '=', count))) pvs
from stats_view_depth_time_platform_session_count
group by date,platform;

-- ==============
2017-05-30      website pv2=1
2017-05-31      android pv2=1
2017-05-31      website pv3=1,pv4=1
2017-07-04      website pv10_30=1
2018-09-19      website pv2=2
2018-10-25      java_server     pv1=1
2018-12-05      website pv10_30=3,pv3=2,pv5_10=3
-- ==============




-- 2. 计算event事件中，计算category和action分组后的记录个数，不涉及到任何的去重操作。
drop table if exists stats_event;
create table stats_event
as
select event_category, event_action, count(1) times
from access_log
where event='e'
group by event_category,event_action;

-- ==============

-- ==============



-- =====================
-- 分析订单相关数据(日期、平台、货币类型、支付方式)
--
-- select * from access_log where event='e_crt'; -- 13
-- select * from access_log where event='e_cs'; -- 8
-- select * from access_log where event='e_cr'; -- 4
--
-- -- 日期: from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd') date
-- -- 平台: platform
-- -- 货币类型: currency_type
-- -- 支付方式: payment_type
--
-- -- 分析订单数量及金额: charge_request事件的  去重后并统计总金额
-- drop table if exists commerce.tmp_order_request_count;
-- create table commerce.tmp_order_request_count
-- as
-- select from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd') date,
--     platform,
--     currency_type,
--     payment_type,
--     count(1) order_request_count,
--     sum(cast(currency_amount as int)) order_request_currency_sum
-- from (select order_id, event,
--         row_number() over(partition by order_id
--                           order by server_time desc) no
--     from commerce.access_log
--     where event='e_crt') t1
-- left join commerce.access_log t2
--     on t1.order_id = t2.order_id and
--        t1.event = t2.event
-- where t1.no=1
-- group by from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd'),
--     platform,
--     currency_type,
--     payment_type;
--
--
-- -- 分析成功订单数量及金额: charge_success事件的 去重后并统计总金额
-- drop table if exists commerce.tmp_order_success_count;
-- create table commerce.tmp_order_success_count
-- as
-- select from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd') date,
--     platform,
--     currency_type,
--     payment_type,
--     count(1) order_success_order_count,
--     sum(cast(currency_amount as int)) order_success_currency_sum
-- from (select order_id, event,
--         row_number() over(partition by order_id
--                           order by server_time desc) no
--     from commerce.access_log
--     where event='e_cs') t1
-- left join commerce.access_log t2
--     on t1.order_id = t2.order_id and
--        t1.event = t2.event
-- where t1.no=1
-- group by from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd'),
--     platform,
--     currency_type,
--     payment_type;
--
--
-- -- 分析退款订单数量及金额: charge_refuse事件的 去重后并统计总金额
-- drop table if exists commerce.tmp_order_refund_count;
-- create table commerce.tmp_order_refund_count
-- as
-- select from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd') date,
--     platform,
--     currency_type,
--     payment_type,
--     count(1) order_refund_order_count,
--     sum(cast(currency_amount as int)) order_refund_currency_sum
-- from (select order_id, event,
--         row_number() over(partition by order_id
--                           order by server_time desc) no
--     from commerce.access_log
--     where event='e_cr') t1
-- left join commerce.access_log t2
--     on t1.order_id = t2.order_id and
--        t1.event = t2.event
-- where t1.no=1
-- group by from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd'),
--     platform,
--     currency_type,
--     payment_type;
--






-- select t1.order_id,t1.date,--
--     t2.platform,
--     t2.currency_type,
--     t2.payment_type,
--     coun-- t(1) order_success_order_count,
--     sum(cast(t2.currency_amount as int)) order_success_currency_sum
-- from (select order_id,
--         from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd') date,
--         row_number() over(partition by order_id
--                           order by server_time desc) no
--     from commerce.access_log
--     where event='e_cs') t1
-- left join (select distinct(order_id),platform,currency_type,payment_type from commerce.access_log where event='e_crt') t2
--     on t1.order_id = t2.order_id
-- where t1.no=1
-- group by t1.order_id,t1.date,
--     t2.platform,
--     t2.currency_type,
--     t2.payment_type;

    

-- 准备表
-- 1. 去重后的 charge request 信息
drop table if exists tmp_charge_request_unique_order;
create table tmp_charge_request_unique_order
as
select distinct(order_id),
    platform,
    currency_type,
    payment_type,
    currency_amount,
    from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd') date 
from commerce.access_log 
where event='e_crt';
  
  
-- 一、 charge request 信息
drop table if exists order_request;
create table order_request
as
select date,
    platform,
    currency_type,
    payment_type,
    count(1) order_request_order_count,
    sum(cast(currency_amount as int)) order_request_currency_sum
from tmp_charge_request_unique_order
group by date,
    platform,
    currency_type,
    payment_type;



-- 二、 charge success 信息
-- 1. 去重后的charge success 订单编号
drop table if exists tmp_charge_success_unique_order;
create table tmp_charge_success_unique_order
as
select distinct(order_id), from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd') date
from commerce.access_log 
where event='e_cs';

-- 2. 拼接计算金额
drop table if exists order_success;
create table order_success
as
select t1.date,
    t2.platform,
    t2.currency_type,
    t2.payment_type,
    count(1) order_success_order_count,
    sum(cast(t2.currency_amount as int)) order_success_currency_sum
from tmp_charge_success_unique_order t1
inner join tmp_charge_request_unique_order t2 on t1.order_id = t2.order_id
group by t1.date,
    t2.platform,
    t2.currency_type,
    t2.payment_type;
    
-- 三、 charge refund 信息
-- 1. 去重后的charge refund 订单编号
drop table if exists tmp_charge_refund_unique_order;
create table tmp_charge_refund_unique_order
as
select distinct(order_id), from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd') date
from commerce.access_log 
where event='e_cr';

-- 2. 拼接计算金额
drop table if exists order_refund;
create table order_refund
as
select t1.date,
    t2.platform,
    t2.currency_type,
    t2.payment_type,
    count(1) order_refund_order_count,
    sum(cast(t2.currency_amount as int)) order_refund_currency_sum
from tmp_charge_refund_unique_order t1
inner join tmp_charge_request_unique_order t2 on t1.order_id = t2.order_id
group by t1.date,
    t2.platform,
    t2.currency_type,
    t2.payment_type;


    
    
    

    
    
    
    
-- 同样的任务需要5个job
select t1.order_id,t1.date,
    t2.platform,
    t2.currency_type,
    t2.payment_type,
    count(1) order_success_order_count,
    sum(cast(t2.currency_amount as int)) order_success_currency_sum
from (select distinct(order_id), from_unixtime(cast(server_time/1000 as bigint), 'yyyy-MM-dd') date
    from commerce.access_log 
    where event='e_cs') t1
left join (select distinct(order_id),platform,currency_type,payment_type,currency_amount from commerce.access_log where event='e_crt') t2 
    on t1.order_id = t2.order_id
group by t1.order_id,t1.date,
    t2.platform,
    t2.currency_type,
    t2.payment_type;

