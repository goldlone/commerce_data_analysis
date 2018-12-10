
-- ======================
--    用户浏览深度
-- ======================

-- 结果表
create table stats_view_depth(
    platform_dimension_id int,
    date_dimension_id int,
    kpi_dimension_id int,
    pv1 bigint,
    pv2 bigint,
    pv3 bigint,
    pv4 bigint,
    pv5_10 bigint,
    pv10_30 bigint,
    pv30_60 bigint,
    `pv60+` bigint
);


-- 1.1 用户维度下的分析
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

-- 一些优化
set hive.exec.mode.local.auto=true; -- 本地模式
set hive.groupby.skewindata=true; -- 预防数据倾斜，并进行负载均衡，在进行查询时会启动两个job，第一个Job会将Map的输出结果随机的分布到reduce中进行部分聚合，第二个job最终完成全部数据的聚合（group by 到同一个reduce中）
set hive.map.aggr=true; -- 在map端进行聚合
set hive.exec.parallel=true; -- job并行执行
set hive.exec.mode.local.auto.input.files.max=7; -- 本地模式支持最大的文件个数，数量不要太大，由于本地模式在同一JVM中执行任务，如果数据量过大会造成OOM(out of memory)
set hive.exec.mode.local.auto.inputbytes.max = 134217728;  -- 128M

-- 1.2 插入结果表
insert into stats_view_depth
select date_udf(date), platform_udf(platform), kpi_udf('activity_user') as kpi, sum(pv1) as pv1, sum(pv2) as pv2, sum(pv3) as pv3, sum(pv4) as pv4, sum(pv5_10) as pv5_10, sum(pv10_30) as pv10_30, sum(pv30_60) as pv30_60, sum(`pv_60+`) as `pv_60+`
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
group by date,platform, 'activity_user';


-- 2.1 Session维度
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


-- 2.2
insert into stats_view_depth
select date_udf(date), platform_udf(platform), kpi_udf('session_count') as kpi, sum(pv1) as pv1, sum(pv2) as pv2, sum(pv3) as pv3, sum(pv4) as pv4, sum(pv5_10) as pv5_10, sum(pv10_30) as pv10_30, sum(pv30_60) as pv30_60, sum(`pv_60+`) as `pv_60+`
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
group by date,platform, 'session_count';



-- 使用Sqoop导出到mysql
sqoop export --connect jdbc:mysql://hh:3306/report \
--username hive --password 123456 --table stats_view_depth \
--export-dir /user/hive/warehouse/commerce.db/stats_view_depth \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,kpi_dimension_id;





