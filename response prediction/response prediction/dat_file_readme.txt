### user_info.dat
user features
format:
[user_id]	[followers_count]	[friends_count]	[followers_count/friends_count]	[listed_count]	[favourites_count]	[statuses_count]

### links.dat
link file, saves all relations used to generate the training dataset
format:
[friend_id]	[user_id]

### user_posts.dat
posts reposted at least once
format:
[user_id]	[post_id]	[hour]
Note: hour is the post published hour (hour of the time)

### reposts.dat
rewteeted posts, difference to user_posts.dat: this file saves all the repost relationship
if a post from user_posts.dat is reposted by two users, then there will be two records
format:
[user_id]	[friend_id]	[post_id]	[repost_id]	[post_hour]	[repost_hour]
Note: the hour in post_hour,repost_hour和user_posts.dat share the same meaning

### i-j-cosine.dat
user i vs. j temporal cosine distance
format:
[friend_id]	[user_id]	[cosine]

### i-j-ratio.dat
post count of user j/user i's all friends post sum count
format:
[user_i]	[user_j]	[ratio]

### timeSeries.dat
normalized variation of user's posts number across hours
format:
[user_i]	[0 1 2 ... 23]

### cosine-sim.dat
user-user topic consine similarity
format:
[friend_id]	[user_id]	[cosine]
