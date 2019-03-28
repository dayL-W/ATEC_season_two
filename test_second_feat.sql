--历史发生金额之和，历史交易次数
set odps.sql.groupby.skewindata=true;
drop table if exists test_small;
create table test_small as
select user_id,gmt_occur, sum(amt) as hour_amt_sum from atec_1000w_ootb_data group by user_id,gmt_occur;

drop table if exists test_history_amt;
create table test_history_amt as
select event_id,user_id,gmt_occur,sum(hour_amt_sum) as sum_hist_amt,  count(*) as count_hist_amt from
(
    select /* + mapjoin(t2)*/
        t2.hour_amt_sum,t1.user_id,t1.gmt_occur,t1.event_id
    from atec_1000w_ootb_data t1  --驱动表
    join test_small t2  --历史行为明细表
        on t1.user_id = t2.user_id 
    where t1.gmt_occur > t2.gmt_occur
) tmp  group by event_id,user_id,gmt_occur;
select * from test_history_amt order by user_id,gmt_occur limit 100;
select * from test_small order by user_id,gmt_occur limit 100;

-- 前3天发生金额之和，历史交易次数
set odps.sql.groupby.skewindata=true;
drop table if exists test_3days_amt;
create table test_3days_amt as
select event_id,user_id,gmt_occur,sum(hour_amt_sum) as sum_3days_amt, count(*) as count_3days_amt from 
(
    select /* + mapjoin(t2)*/
        t2.hour_amt_sum,t1.user_id,t1.gmt_occur,t1.event_id
    from atec_1000w_ootb_data t1  --驱动表
    join test_small t2  --历史行为明细表
        on t1.user_id = t2.user_id 
    where t1.gmt_occur > t2.gmt_occur and substr(dateadd(concat(t1.gmt_occur,':00:00'),-4,'dd'),1,10) < t2.gmt_occur
) tmp  group by event_id,user_id,gmt_occur;
select * from test_3days_amt  where user_id='00007a4d1b265a2d14d2b742a136a2acff6f4760fc8aa40aae7c80579e537a98' order by gmt_occur limit 100;
select * from test_small where user_id='00007a4d1b265a2d14d2b742a136a2acff6f4760fc8aa40aae7c80579e537a98' order by gmt_occur limit 100;
