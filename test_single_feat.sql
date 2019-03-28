drop table if exists test_single_feat;
create table test_single_feat as
select 
    user_id, client_ip, network, device_sign, ip_prov, ip_city,
    card_bin_prov,card_bin_city,mobile_oper_platform,operation_channel,
    pay_scene, amt,event_id,gmt_occur,
    --对is_peer_pay编码
    case
        when is_peer_pay='85d8c6ac08d5eb8a634919182f6699d1889df0364531d1425a864a00af5571ba' then 0
        when is_peer_pay='d57835fe5b36ac9cf1f4e905462e51a0cb58bb9d7e167cc70c44834392bf2138' then 1
        else -1
    end as is_peer_pay_encoding,
    --对is_one_people编码
    case 
        when is_one_people='97ac3bcec6013ea37e8180a267f17a8cf8cccadec130d6f54a673e24151d4799' then 0
        when is_one_people='ce8d628e0da6ca9872afb848fe4f0c87b45ee8c45d7b2a1cf3c0f0398e99af31' then 1
    end as is_one_people_encoding,
    --判断info-1是否为空
    case 
        when info_1 is null then 0
        else  1
    end as info_1_null,
    --判断info-2是否为空
    case 
        when info_2 is null then 0
        else  1
    end as info_2_null,
    --判断card_cert_no是否为空
    case 
        when card_cert_no is null then 0
        else  1
    end as card_cert_no_null,
    --判断income_card_no
    case 
        when income_card_no is null then 0
        else  1
    end as income_card_no_null,
    --判断income_card_cert_no是否为空
    case 
        when income_card_cert_no is null then 0
        else  1
    end as income_card_cert_no_null,
    --判断income_card_mobile是否为空
    case 
        when income_card_mobile is null then 0
        else  1
    end as income_card_mobile_null,
    --判断income_card_bank_code是否为空
    case 
        when income_card_bank_code is null then 0
        else  1
    end as income_card_bank_code_null,
    --判断province是否为空
    case 
        when province is null then 0
        else  1
    end as province_null,
    --判断city是否为空
    case 
        when city is null then 0
        else  1
    end as city_null,
    --判断这笔交易是否发生在凌晨
    case when substr(gmt_occur,12,13)>='01' and substr(gmt_occur,12,13)<='05' then 1
        else 0
    end as is_morning,
    --对用户的交易时间段做划分
    case when (substr(gmt_occur,12,13)>='09' and substr(gmt_occur,12,13)<='11') or (substr(gmt_occur,12,13)>='14' and substr(gmt_occur,12,13)<='17') then 'working'
    when (substr(gmt_occur,12,13)>='06' and substr(gmt_occur,12,13)<='08') or (substr(gmt_occur,12,13)>='12' and substr(gmt_occur,12,13)<='13') or (substr(gmt_occur,12,13)>='18' and substr(gmt_occur,12,13)<='20') then 'eating'
    when (substr(gmt_occur,12,13)>='21' and substr(gmt_occur,12,13)<='23') then 'evening'
    when (substr(gmt_occur,12,13)>='00' and substr(gmt_occur,12,13)<='05') then 'moring'
    end as time_split
from atec_1000w_ootb_data;

select * from test_single_feat;