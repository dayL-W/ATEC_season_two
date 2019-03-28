--select count(DISTINCT  is_fraud) as dd,count(DISTINCT  info_2) as info_2,count(DISTINCT  info_1) as info_1 ,count(DISTINCT  user_id) as user_id ,count(DISTINCT  opposing_id) as opposing_id from atec_1000w_ins_data;
--select info_2, count(  info_2) as dd  from atec_1000w_ins_data group by info_2;
--select info_1, count( info_1) as dd  from atec_1000w_ins_data group by info_1;
select province,city, count(*) as dd  from atec_1000w_ins_data where is_fraud>=0 group by province,city order by province;
drop table if exists version_sum;
create table version_sum as select version,count(*) as sum_ver from atec_1000w_ins_data group by version;
select * from version_sum;

drop table if exists version_fruad;
create table version_fruad as select version,is_fraud, count(*) as count_ver from atec_1000w_ins_data group by version, is_fraud;
select * from version_fruad;

drop table if exists version_prob;
create table version_prob as
select a.version,  a.count_ver, a.is_fraud,
        (a.count_ver*1.0 / b.sum_ver ) as prob_version
        from version_fruad a
        left outer join version_sum b
            on a.version = b.version;
select * from version_prob order by is_fraud ,prob_version desc limit 100;