drop table if exists result_neg;
create table result_neg as select event_id,cast(get_json_object(prediction_detail,'$.1') as double) as score from neg1_predict order by score desc limit 16934;
select * from result_neg limit 200; 

drop table if exists result_add;
create table result_add as 
select event_id,
        case when rank_score<=int(0.65*16934) then 1
        else 0
        end as is_fraud
from
(
    select *,rank() over(order by score desc) as rank_score from result_neg
)t;

select * from result_add;