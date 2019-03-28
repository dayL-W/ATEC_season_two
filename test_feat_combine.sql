drop table if exists test_feat;
create table test_feat as
    select 
    a.amt, a.is_peer_pay_encoding, a.is_one_people_encoding,a.event_id,
    a.info_1_null,a.info_2_null,a.card_cert_no_null,a.income_card_no_null,
    a.income_card_cert_no_null,a.income_card_mobile_null,a.income_card_bank_code_null,
    a.province_null,a.city_null,a.is_morning,
    b.count_ip, b.prob_ip,
    c.count_network, c.prob_network,
    d.count_device, d.prob_device,
    e.count_prov, e.prob_prov,
    f.count_city, f.prob_city,
    g.count_card_bin_prov, g.prob_card_bin_prov,
    h.count_card_bin_city, h.prob_card_bin_city,
    i.count_mobile_oper_platform, i.prob_mobile_oper_platform,
    j.count_operation_channel, j.prob_operation_channel,
    k.count_pay_scene, k.prob_pay_scene,
    l.count_amt, l.prob_amt,
    m.sum_hist_amt, m.count_hist_amt,
    n.sum_3days_amt, n.count_3days_amt,
    o.split_prob,
    k.prob_pay_scene*l.prob_amt as scene_amt
    
    from test_single_feat a 
    left outer join 
        test_ip_prob b
        on a.user_id = b.user_id and (a.client_ip = b.client_ip or a.client_ip is null and b.client_ip is null)
    left outer join
        test_network_prob c 
        on a.user_id = c.user_id and (a.network = c.network or a.network is null and c.network is null)
    left outer join
        test_device_prob d 
        on a.user_id = d.user_id and (a.device_sign = d.device_sign or a.device_sign is null and d.device_sign is null)
    left outer join
        test_prov_prob e 
        on a.user_id = e.user_id and (a.ip_prov = e.ip_prov or a.ip_prov is null and e.ip_prov is null)
    left outer join
        test_city_prob f 
        on a.user_id = f.user_id and (a.ip_city = f.ip_city or a.ip_city is null and f.ip_city is null)
    left outer join
        test_card_bin_prov_prob g 
        on a.user_id = g.user_id and (a.card_bin_prov = g.card_bin_prov or a.card_bin_prov is null and g.card_bin_prov is null)
    left outer join
        test_card_bin_city_prob h 
        on a.user_id = h.user_id and (a.card_bin_city = h.card_bin_city or a.card_bin_city is null and h.card_bin_city is null)
    left outer join
        test_mobile_oper_platform_prob i 
        on a.user_id = i.user_id and (a.mobile_oper_platform = i.mobile_oper_platform or a.mobile_oper_platform is null and i.mobile_oper_platform is null)
    left outer join
        test_operation_channel_prob j 
        on a.user_id = j.user_id and (a.operation_channel = j.operation_channel or a.operation_channel is null and j.operation_channel is null)
    left outer join
        test_pay_scene_prob k 
        on a.user_id = k.user_id and (a.pay_scene = k.pay_scene or a.pay_scene is null and k.pay_scene is null)
    left outer join
        test_amt_prob l 
        on a.user_id = l.user_id and (a.amt = l.amt or a.amt is null and l.amt is null)
    left outer join
        test_history_amt m 
        on a.event_id = m.event_id
    left outer join
        test_3days_amt n 
        on a.event_id = n.event_id
    left outer join
        test_split_prob o 
        on a.user_id = o.user_id and a.time_split=o.time_split;
select * from test_feat;