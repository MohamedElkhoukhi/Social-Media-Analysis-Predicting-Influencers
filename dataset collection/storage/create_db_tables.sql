sudo stop mysql
sudo start mysql
**************************************************
-- data set: extract 1w users' infos from the Illinois dataset 
use facebook;

-- user profile
create table facebook_user
(
  id varchar(128) not null,
  followers_count int unsigned default 0,
  listed_count int unsigned default 0, --  	The number of public lists that this user is a member of.
  utc_offset varchar(32) not null,
  statuses_count int unsigned default 0,
  description varchar(512),
  friends_count int unsigned default 0,
  location varchar(512),
  geo_enabled boolean,
  name varchar(512),
  lang varchar(64),
  favourites_count int unsigned default 0, -- The number of posts this user has favorited in the account's lifetime. British spelling used in the field name for historical reasons. 
  screen_name varchar(512),
  created_at datetime,
  time_zone varchar(128),
  protected boolean,
  primary key(id)
)engine=innodb default charset=utf8;
ALTER TABLE facebook_user ADD INDEX index_facebookuser (id);

-- posts
create table facebook_posts
(
  text varchar(2048) not null,
  source_url varchar(1024),
  id varchar(128) not null,
  favorite_count int unsigned default 0,
  source varchar(1024),
  lang varchar(64),
  user_id varchar(128) not null,
  reposted_status varchar(128),
  created_at datetime,
  reposted boolean,
  symbols varchar(256),
  user_mentions varchar(1024),
  hashtags varchar(1024),
  urls varchar(1024),
  in_reply_to_status_id_str varchar(128),
  repost_count int unsigned default 0,
  in_reply_to_user_id varchar(128),
  primary key(id)
)engine=innodb default charset=utf8;
ALTER TABLE facebook_posts ADD INDEX index_facebookposts (id);
ALTER TABLE facebook_posts ADD INDEX index_facebookposts_userid (user_id);
ALTER TABLE facebook_posts ADD INDEX index_facebookposts_repostid (reposted_status);

-- reposts
create table facebook_reposts
(
  text varchar(2048) not null,
  source_url varchar(1024),
  id varchar(128) not null,
  favorite_count int unsigned default 0,
  source varchar(1024),
  lang varchar(64),
  user_id varchar(128) not null,
  created_at datetime,
  reposted boolean,
  symbols varchar(256),
  user_mentions varchar(1024),
  hashtags varchar(1024),
  urls varchar(1024),
  in_reply_to_status_id_str varchar(128),
  repost_count int unsigned default 0,
  in_reply_to_user_id varchar(128),
  primary key(id)
)engine=innodb default charset=utf8;
ALTER TABLE facebook_reposts ADD INDEX index_facebookreposts (id);

-- count posts by hour and week day
create table post_count
(
  user_id varchar(128) not null,
  week_day int unsigned default 0,
  hour int unsigned default 0,
  total_count float default 0,
  repost_count float default 0,
  reply_count float default 0,
  primary key(user_id,week_day,hour) 
)engine=innodb default charset=utf8;

ALTER TABLE post_count ADD INDEX index_postcount (user_id);

-- relation
create table user_friend
(
  user_id varchar(128) not null,
  friend_id varchar(128) not null,
  isCollected boolean,
  primary key(user_id,friend_id)
)engine=innodb default charset=utf8;
ALTER TABLE user_friend ADD INDEX index_userfriend (user_id,friend_id);

