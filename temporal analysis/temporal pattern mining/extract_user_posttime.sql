## user_post_time 
-- record each post's user_id and created time
create table user_post_time
(
  user_id varchar(128) not null,
  post_id varchar(128) not null,
  created_at int(11) default 0,
  primary key(user_id,post_id)
)engine=innodb default charset=utf8;

ALTER TABLE user_post_time ADD INDEX index_userposttime (user_id,post_id);

mysql> insert ignore into user_post_time (select user_id, id as post_id, UNIX_TIMESTAMP(created_at) as created_at from twitter_posts);
