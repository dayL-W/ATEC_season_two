--cast(get_json_object(prediction_detail,'$.1') as double) as score

drop table if exists result;
create table result as select event_id as id,cast(get_json_object(prediction_detail,'$.1') as double) as score from result_temp;
select * from result order by id desc limit 200;