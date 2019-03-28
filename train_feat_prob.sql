drop table if exists train_trade_sum;
create table train_trade_sum as
select user_id, count(*) trade_sum from atec_1000w_ins_data  group by user_id;

--统计训练数据中每个用户下所有IP的交易次数
drop table if exists train_ip_count;
create table train_ip_count as
select user_id, client_ip, count(*) as count_ip from atec_1000w_ins_data group by user_id,client_ip;

--通过上面两个表计算出每个用户不同IP的使用概率
drop table if exists train_ip_prob;
create table train_ip_prob as
select i.user_id as user_id, i.client_ip as client_ip, i.count_ip as count_ip, 
        (i.count_ip*1.0 / t.trade_sum ) as prob_ip
        from train_ip_count i 
        left outer join train_trade_sum t 
            on i.user_id = t.user_id;

--统计训练数据中每个用户下所有network的交易次数
drop table if exists train_network_count;
create table train_network_count as
select user_id, network, count(*) as count_network from atec_1000w_ins_data  group by user_id,network;

--通过上面两个表计算出每个用户不同network的使用概率
drop table if exists train_network_prob;
create table train_network_prob as
select i.user_id as user_id, i.network as network, i.count_network as count_network, 
        (i.count_network*1.0 / t.trade_sum ) as prob_network
        from train_network_count i 
        left outer join train_trade_sum t 
            on i.user_id = t.user_id;
            
--统计训练数据中每个用户下所有device的交易次数
drop table if exists train_device_count;
create table train_device_count as
select user_id, device_sign, count(*) as count_device from atec_1000w_ins_data  group by user_id,device_sign;

--通过上面两个表计算出每个用户不同device的使用概率
drop table if exists train_device_prob;
create table train_device_prob as
select i.user_id as user_id, i.device_sign as device_sign, i.count_device as count_device, 
        (i.count_device*1.0 / t.trade_sum ) as prob_device
        from train_device_count i 
        left outer join train_trade_sum t 
            on i.user_id = t.user_id;
--统计训练数据中每个用户下所有prov的交易次数
drop table if exists train_prov_count;
create table train_prov_count as
select user_id, ip_prov, count(*) as count_prov from atec_1000w_ins_data  group by user_id,ip_prov;

--通过上面两个表计算出每个用户不同prov的使用概率
drop table if exists train_prov_prob;
create table train_prov_prob as
select i.user_id as user_id, i.ip_prov as ip_prov, i.count_prov as count_prov, 
        (i.count_prov*1.0 / t.trade_sum ) as prob_prov
        from train_prov_count i 
        left outer join train_trade_sum t 
            on i.user_id = t.user_id;
--统计训练数据中每个用户下所有city的交易次数
drop table if exists train_city_count;
create table train_city_count as
select user_id, ip_city, count(*) as count_city from atec_1000w_ins_data  group by user_id,ip_city;

--通过上面两个表计算出每个用户不同city的使用概率
drop table if exists train_city_prob;
create table train_city_prob as
select i.user_id as user_id, i.ip_city as ip_city, i.count_city as count_city, 
        (i.count_city*1.0 / t.trade_sum ) as prob_city
        from train_city_count i 
        left outer join train_trade_sum t 
            on i.user_id = t.user_id;
            
--统计训练数据中每个用户下所有card_bin_prov的交易次数
drop table if exists train_card_bin_prov_count;
create table train_card_bin_prov_count as
select user_id, card_bin_prov, count(*) as count_card_bin_prov from atec_1000w_ins_data  group by user_id,card_bin_prov;


--通过上面两个表计算出每个用户不同card_bin_prov的使用概率
drop table if exists train_card_bin_prov_prob;
create table train_card_bin_prov_prob as
select i.user_id as user_id, i.card_bin_prov as card_bin_prov, i.count_card_bin_prov as count_card_bin_prov, 
        (i.count_card_bin_prov*1.0 / t.trade_sum ) as prob_card_bin_prov
        from train_card_bin_prov_count i 
        left outer join train_trade_sum t 
            on i.user_id = t.user_id;
--统计训练数据中每个用户下所有card_bin_city的交易次数
drop table if exists train_card_bin_city_count;
create table train_card_bin_city_count as
select user_id, card_bin_city, count(*) as count_card_bin_city from atec_1000w_ins_data  group by user_id,card_bin_city;


--通过上面两个表计算出每个用户不同card_bin_city的使用概率
drop table if exists train_card_bin_city_prob;
create table train_card_bin_city_prob as
select i.user_id as user_id, i.card_bin_city as card_bin_city, i.count_card_bin_city as count_card_bin_city, 
        (i.count_card_bin_city*1.0 / t.trade_sum ) as prob_card_bin_city
        from train_card_bin_city_count i 
        left outer join train_trade_sum t 
            on i.user_id = t.user_id;
--统计训练数据中每个用户下所有mobile_oper_platform的交易次数
drop table if exists train_mobile_oper_platform_count;
create table train_mobile_oper_platform_count as
select user_id, mobile_oper_platform, count(*) as count_mobile_oper_platform from atec_1000w_ins_data  group by user_id,mobile_oper_platform;

--通过上面两个表计算出每个用户不同card_bin_city的使用概率
drop table if exists train_mobile_oper_platform_prob;
create table train_mobile_oper_platform_prob as
select i.user_id as user_id, i.mobile_oper_platform as mobile_oper_platform, i.count_mobile_oper_platform as count_mobile_oper_platform, 
        (i.count_mobile_oper_platform*1.0 / t.trade_sum ) as prob_mobile_oper_platform
        from train_mobile_oper_platform_count i 
        left outer join train_trade_sum t 
            on i.user_id = t.user_id;
--统计训练数据中每个用户下所有operation_channel的交易次数
drop table if exists train_operation_channel_count;
create table train_operation_channel_count as
select user_id, operation_channel, count(*) as count_operation_channel from atec_1000w_ins_data group by user_id,operation_channel;

--通过上面两个表计算出每个用户不同operation_channel的使用概率
drop table if exists train_operation_channel_prob;
create table train_operation_channel_prob as
select i.user_id as user_id, i.operation_channel as operation_channel, i.count_operation_channel as count_operation_channel, 
        (i.count_operation_channel*1.0 / t.trade_sum) as prob_operation_channel
        from train_operation_channel_count i 
        left outer join train_trade_sum t 
            on i.user_id = t.user_id;
--统计训练数据中每个用户下所有pay_scene的交易次数
drop table if exists train_pay_scene_count;
create table train_pay_scene_count as
select user_id, pay_scene, count(*) as count_pay_scene from atec_1000w_ins_data group by user_id,pay_scene;

--通过上面两个表计算出每个用户不同pay_scene的使用概率
drop table if exists train_pay_scene_prob;
create table train_pay_scene_prob as
select i.user_id as user_id, i.pay_scene as pay_scene, i.count_pay_scene as count_pay_scene, 
        (i.count_pay_scene*1.0 / t.trade_sum) as prob_pay_scene
        from train_pay_scene_count i 
        left outer join train_trade_sum t 
            on i.user_id = t.user_id;
            
--统计训练数据中每个用户下所有amt的交易次数
drop table if exists train_amt_count;
create table train_amt_count as
select user_id, amt, count(*) as count_amt from atec_1000w_ins_data group by user_id,amt;

--通过上面两个表计算出每个用户不同amt的使用概率
drop table if exists train_amt_prob;
create table train_amt_prob as
select i.user_id as user_id, i.amt as amt, i.count_amt as count_amt, 
        (i.count_amt*1.0 / t.trade_sum) as prob_amt
        from train_amt_count i 
        left outer join train_trade_sum t 
            on i.user_id = t.user_id;
            
--统计每个用户交易时间段的个数
drop table if exists train_split_count;
create table train_split_count as
select user_id,time_split,count(*) as split_count
from train_single_feat group by user_id,time_split;

--统计每个用户每个交易时间段的概率
drop table if exists train_split_prob;
create table train_split_prob as
select a.user_id,a.time_split,a.split_count,b.trade_sum,
        (a.split_count * 1.0 / b.trade_sum) as split_prob
from train_split_count a 
left outer join 
    train_trade_sum b
    on a.user_id=b.user_id;