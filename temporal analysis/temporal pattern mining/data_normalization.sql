-- count posts by hour
create table post_count_hour
(
  user_id varchar(128) not null,
  hour int unsigned default 0,
  total_count float default 0,
  primary key(user_id,hour)
)engine=innodb default charset=utf8;
ALTER TABLE post_count_hour ADD INDEX index_postcounthour (user_id,hour);

create table post_count_hour_tmp (select user_id,max(total_count) max,min(total_count) min from post_count_hour group by user_id);

-- count posts by hour and normalized
create table post_count_hour_normalized
(
  user_id varchar(128) not null,
  hour int unsigned default 0,
  total_count float default 0,
  primary key(user_id,hour)
)engine=innodb default charset=utf8;
ALTER TABLE post_count_hour_normalized ADD INDEX index_postcounthournormalized (user_id,hour);

-- count posts by hour divide by the amount of hours
create table post_count_hour_avg
(
  user_id varchar(128) not null,
  hour int unsigned default 0,
  total_count float default 0,
  primary key(user_id,hour)
)engine=innodb default charset=utf8;
ALTER TABLE post_count_hour_avg ADD INDEX index_postcounthouravg (user_id,hour);

-- count posts by week day
create table post_count_day
(
  user_id varchar(128) not null,
  day int unsigned default 0,
  total_count float default 0,
  primary key(user_id,day)
)engine=innodb default charset=utf8;
ALTER TABLE post_count_day ADD INDEX index_postcountday (user_id,day);


mysql> insert into post_count_hour (select user_id,hour,sum(total_count) as total_count from post_count group by user_id,hour);
mysql> insert into post_count_hour_avg (select * from post_count_hour);


mysql> update post_count_hour_avg set total_count=total_count/30.0 where user_id in (select distinct user_id from user_habit where post_count<3200);
mysql> UPDATE post_count_hour_avg INNER JOIN user_habit ON post_count_hour_avg.user_id=user_habit.user_id SET total_count=total_count*1.0/days WHERE user_habit.post_count=3200;
	

mysql> create table post_count_hour_tmp (select user_id,max(total_count) max,min(total_count) min from post_count_hour group by user_id);
mysql> insert into post_count_hour_normalized (select * from post_count_hour);(Õâ¸öµØ·½ÓÃpost_count_hour_avgÒ²¿ÉÒÔ)
mysql> UPDATE post_count_hour_normalized LEFT JOIN post_count_hour_tmp ON post_count_hour_normalized.user_id=post_count_hour_tmp.user_id SET total_count=total_count/max;

## mysql> insert ignore into post_count_day (select user_id,week_day,sum(total_count) as total_count from post_count group by user_id,week_day);

mysql> update post_count_day set total_count=total_count/4.0 where user_id in (select distinct user_id from user_habit where post_count<3200);
mysql> UPDATE post_count_day INNER JOIN user_habit ON post_count_day.user_id=user_habit.user_id SET total_count=total_count*1.0/days*7.0 WHERE user_habit.post_count=3200;
