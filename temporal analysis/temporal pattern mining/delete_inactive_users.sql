###### delete users whose does have a 'following' relationship with the other users
mysql> select distinct A.id  from facebook_user A where id not in (select distinct user_id as id from user_friend) and id not in (select distinct friend_id as id from user_friend);
mysql> delete from facebook_user where id not in (select distinct user_id as id from user_friend) and id not in (select distinct friend_id as id from user_friend);
###### delete users who have ever published less than 20 posts

insert ignore into post_count_user (select user_id, sum(total_count) as total_count from post_count group by user_id)
mysql> select distinct user_id from post_count where user_id not in (select distinct(id) as id from facebook_user)
459

	###### delete users whose profile is not collected successfully
	mysql> delete from post_count where user_id not in (select distinct(id) as id from facebook_user);
	mysql> delete from post_count_user where user_id not in (select distinct(id) as id from facebook_user);

mysql> select count(*) from post_count_user where total_count>20;
+----------+
| count(*) |
+----------+
|     7230 |
+----------+
1 row in set (0.00 sec)
mysql> delete from post_count_user where total_count <20;
mysql> delete from post_count where user_id not in (select distinct(user_id) as id from post_count_user);

###### ..............
delete from facebook_user where id not in (select distinct(user_id) as id from post_count_user);
delete from user_friend where user_id not in (select distinct(id) as id from facebook_user) or friend_id not in (select distinct(id) as id from facebook_user);
delete from facebook_posts where user_id not in (select distinct(id) as id from facebook_user);
delete from facebook_reposts where user_id not in (select distinct(id) as id from facebook_user);
***********************************
##### check the remaining users and posts
***********************************
mysql> select count(*) from facebook_user;
7247

mysql> select count(*) from facebook_posts;
3541214

mysql> select count(*) from facebook_posts where reposted_status != 'null';
600943

mysql> select count(*) from facebook_reposts;
569863

mysql> select sum(repost_count) from post_count;
600943

mysql> select sum(reply_count) from post_count;
787688
