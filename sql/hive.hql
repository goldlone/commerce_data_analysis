
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

-- ==============
2017-05-30      website pv2=1
2017-05-31      android pv1=2
2017-05-31      website pv1=2,pv5_10=1
2017-07-04      website pv10_30=1
2018-09-19      website pv2=2
2018-10-25      java_server     pv1=1
2018-12-05      website pv10_30=4,pv3=2,pv5_10=1
-- ==============



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

