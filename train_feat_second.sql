-- 历史发生金额之和，历史交易次数
set odps.sql.groupby.skewindata=true;

drop table if exists train_small;
create table train_small as
select user_id,gmt_occur, sum(amt) as hour_amt_sum from atec_1000w_ins_data where gmt_occur<'2017-10-01 00' or gmt_occur>='2017-10-08 23' group by user_id,gmt_occur;

drop table if exists train_history_amt;
create table train_history_amt as
select event_id,user_id,gmt_occur,sum(hour_amt_sum) as sum_hist_amt,  count(*) as count_hist_amt from
(
    select /* + mapjoin(train_small) */
        t2.hour_amt_sum,t1.user_id,t1.gmt_occur,t1.event_id
    from (select * from atec_1000w_ins_data where gmt_occur<'2017-10-01 00' or gmt_occur>='2017-10-08 23') t1  --驱动表
    join train_small t2  --历史行为明细表
        on t1.user_id = t2.user_id 
    where t1.gmt_occur > t2.gmt_occur
) t group  by event_id,user_id,gmt_occur;
select * from train_history_amt  where user_id='000030d2c6c61ac340ea3b519e573659f0f2954859d3f5a4809861122d92cc7c' order by gmt_occur limit 200;
select * from train_small where user_id='000030d2c6c61ac340ea3b519e573659f0f2954859d3f5a4809861122d92cc7c' order by gmt_occur limit 200;

-- 前3天发生金额之和，历史交易次数
set odps.sql.groupby.skewindata=true;
drop table if exists train_3days_amt;
create table train_3days_amt as
select event_id,user_id,gmt_occur,sum(hour_amt_sum) as sum_3days_amt, count(*) as count_3days_amt from 
(
    select /* + mapjoin(train_small) */
        t2.hour_amt_sum,t1.user_id,t1.gmt_occur,t1.event_id
    from (select * from atec_1000w_ins_data where gmt_occur<'2017-10-01 00' or gmt_occur>='2017-10-08 23') t1  --驱动表
    join train_small t2  --历史行为明细表
        on t1.user_id = t2.user_id 
    where t1.gmt_occur > t2.gmt_occur and substr(dateadd(concat(t1.gmt_occur,':00:00'),-4,'dd'),1,10) < t2.gmt_occur
)t group by event_id,user_id,gmt_occur;
select * from train_3days_amt  where user_id='000030d2c6c61ac340ea3b519e573659f0f2954859d3f5a4809861122d92cc7c' order by gmt_occur limit 200;
select * from train_small where user_id='000030d2c6c61ac340ea3b519e573659f0f2954859d3f5a4809861122d92cc7c' order by gmt_occur limit 200;