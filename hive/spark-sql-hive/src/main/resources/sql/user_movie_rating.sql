CREATE TABLE twq.ad_hoc_query(
    age int,
    gender string,
    occupation string
) STORED AS parquet;


INSERT OVERWRITE TABLE twq.ad_hoc_query
select age, gender, occupation from twq.u_user u
    left join u_data d on u.user_id = d.user_id
    join u_item i on d.item_id = i.movie_id
where d.rating > 3 and i.action = 1
group by age, gender, occupation;

