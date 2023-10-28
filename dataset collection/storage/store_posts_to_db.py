#! /usr/bin/python
# -*- coding: utf-8 -*-
# store post of separate magnitudes into redis. 

import os,json
import MySQLdb
from datetime import *
import dateutil.parser as dp

def convert_to_post(repost):
	try:
		post = []
		## text
		if repost['text'] is None:
  			post.append('null')
  		else:
  			post.append(repost['text'].encode('utf-8'))
  		## source_url
  		if repost['source_url'] is None:
  			post.append('null')
  		else:
  			post.append(repost['source_url'].encode('utf-8'))
  		## id
  		post.append(repost['id'])
  		## favorite_count
  		post.append(repost['favorite_count'])
  		## source
  		if repost['source'] is None:
  			post.append('null')
  		else:
  			post.append(repost['source'].encode('utf-8'))
  		## lang
  		if repost['lang'] is None:
  			post.append('null')
  		else:
  			post.append(repost['lang'].encode('utf-8'))
  		## user_id
  		post.append(repost['user_id'])
  		## created_at
  		if repost['created_at'] is None:
  			post.append('null')
  		else:
  			standard_time = dp.parse(repost['created_at'])
			post.append(standard_time.strftime('%Y-%m-%d %H:%M:%S'))
		## reposted by the authorized user
		if repost['reposted']:
  			post.append('1')
  		else:
  			post.append('0')
  		 				
  		if repost['entities'] is None:
  			post.append('null') ## symbols 
  			post.append('null') ## user_mentions
  			post.append('null') ## hashtags
  			post.append('null') ## urls
  		else:
  			entities = repost['entities']
  			## symbols 
  			if entities['symbols'] is None or len(entities['symbols']) == 0:
  				post.append('null')
  			else:
  				symbols = ''
  				for symbol in entities['symbols']:
  					symbols = symbols + symbol['text'] + ','
  				symbols = symbols.encode('utf-8')					
  				post.append(symbols[0:len(symbols)-1])
  			## user_mentions 
  			if entities['user_mentions'] is None or len(entities['user_mentions']) == 0:
  				post.append('null')
  			else:
  				user_mentions = ''
  				for mention in entities['user_mentions']:
  					user_mentions = user_mentions + str(mention['id']) + ','
  				user_mentions = user_mentions.encode('utf-8')  						
  				post.append(user_mentions[0:len(user_mentions)-1])
  			## hashtags 
  			if entities['hashtags'] is None or len(entities['hashtags']) == 0:
  				post.append('null')
  			else:
  				hashtags = ''
  				for tag in entities['hashtags']:
  					hashtags = hashtags + tag['text'] + ','
  				hashtags = hashtags.encode('utf-8')  						
  				post.append(hashtags[0:len(hashtags)-1])
  			## urls 
  			if entities['urls'] is None or len(entities['urls']) == 0:
  				post.append('null')
  			else:
  				urls = ''
  				for url in entities['urls']:
  					urls = urls + url['url'] + ','
  				urls = urls.encode('utf-8')  						
  				post.append(urls[0:len(urls)-1])
  		## in_reply_to_status_id_str varchar(128)
  		if repost['in_reply_to_status_id_str'] is None:
  			post.append('null')
  		else:
  			post.append(repost['in_reply_to_status_id_str'].encode('utf-8'))
  		## repost_count,
  		post.append(repost['repost_count'])
  		## in_reply_to_user_id varchar(128),
  		if repost['in_reply_to_user_id'] is None:
  			post.append('null')
  		else:
  			post.append(repost['in_reply_to_user_id'])
		return post
		#print post
		#break
	except Exception, e:
	        print 'exception:user %s'%user_id
	        print repost
	        return None
	        
# create database connection
try:
	conn=MySQLdb.connect(host='localhost',user='root',passwd='root',db='twitter',port=3306)
	cur=conn.cursor()
except MySQLdb.Error,e:
     	print "Mysql Error %d: %s" % (e.args[0], e.args[1])

fo = open('../log/store-posts-success.txt','r')
users_done = []
for line in fo:
	tmp_line = line.strip()
	if tmp_line != '':
		users_done.append(tmp_line)
fo.close()

fi = open('../log/store-posts-success.txt','a')
count = 0
file_list = os.listdir('../posts/')
for post_file in file_list:
	f = open('../posts/' + post_file, 'r')
	user_id = post_file
	# check if done already
	if user_id in users_done:
		continue		
	posts=[]
	reposts=[]
	total_count = [[0 for i in range(24)] for j in range(7)]
	repost_count = [[0 for i in range(24)] for j in range(7)]
	reply_count = [[0 for i in range(24)] for j in range(7)]
	for line in f:
		tmp = line.strip()
		if tmp == '':
			print 'The end of posts file...'
		else:
			try:
				tmp = json.loads(tmp)
				post = []
				## text
				if tmp['text'] is None:
  					post.append('null')
  				else:
  					post.append(tmp['text'].encode('utf-8'))
  				## source_url
  				if tmp['source_url'] is None:
  					post.append('null')
  				else:
  					post.append(tmp['source_url'].encode('utf-8'))
  				## id
  				post.append(tmp['id'])
  				## favorite_count
  				post.append(tmp['favorite_count'])
  				## source
  				if tmp['source'] is None:
  					post.append('null')
  				else:
  					post.append(tmp['source'].encode('utf-8'))
  				## lang
  				if tmp['lang'] is None:
  					post.append('null')
  				else:
  					post.append(tmp['lang'].encode('utf-8'))
  				## user_id
  				post.append(tmp['user_id'])
  				## reposted_status
  				if 'reposted_status' in tmp:
  					repost = convert_to_post(tmp['reposted_status'])
  					if repost:
  						reposts.append(repost)
  					post.append(tmp['reposted_status']['id'])
  				else:
  					post.append('null')
  				## created_at
  				if tmp['created_at'] is None:
  					post.append('null')
  				else:
  					standard_time = dp.parse(tmp['created_at'])
					post.append(standard_time.strftime('%Y-%m-%d %H:%M:%S'))
					# count the number of posts by hour and week day
					week_day = standard_time.weekday() #Monday is 0 and Sunday is 6
					hour = standard_time.hour # 0: [00:00 - 1:00] 1: [1:00 -- 2:00]
					total_count[week_day][hour] = total_count[week_day][hour] + 1
					if 'reposted_status' in tmp:
						repost_count[week_day][hour] = repost_count[week_day][hour] + 1
					if tmp['in_reply_to_status_id_str'] is not None:
						reply_count[week_day][hour] = reply_count[week_day][hour] + 1
				## reposted by the authorized user
				if tmp['reposted']:
  					post.append('1')
  				else:
  					post.append('0')
  				 				
  				if tmp['entities'] is None:
  					post.append('null') ## symbols 
  					post.append('null') ## user_mentions
  					post.append('null') ## hashtags
  					post.append('null') ## urls
  				else:
  					entities = tmp['entities']
  					## symbols 
  					if entities['symbols'] is None or len(entities['symbols']) == 0:
  						post.append('null')
  					else:
  						symbols = ''
  						for symbol in entities['symbols']:
  							symbols = symbols + symbol['text'] + ','
  						symbols = symbols.encode('utf-8')					
  						post.append(symbols[0:len(symbols)-1])
  					## user_mentions 
  					if entities['user_mentions'] is None or len(entities['user_mentions']) == 0:
  						post.append('null')
  					else:
  						user_mentions = ''
  						for mention in entities['user_mentions']:
  							user_mentions = user_mentions + str(mention['id']) + ','
  						user_mentions = user_mentions.encode('utf-8')  						
  						post.append(user_mentions[0:len(user_mentions)-1])
  					## hashtags 
  					if entities['hashtags'] is None or len(entities['hashtags']) == 0:
  						post.append('null')
  					else:
  						hashtags = ''
  						for tag in entities['hashtags']:
  							hashtags = hashtags + tag['text'] + ','
  						hashtags = hashtags.encode('utf-8')  						
  						post.append(hashtags[0:len(hashtags)-1])
  					## urls 
  					if entities['urls'] is None or len(entities['urls']) == 0:
  						post.append('null')
  					else:
  						urls = ''
  						for url in entities['urls']:
  							urls = urls + url['url'] + ','
  						urls = urls.encode('utf-8')					
  						post.append(urls[0:len(urls)-1])
  				## in_reply_to_status_id_str varchar(128)
  				if tmp['in_reply_to_status_id_str'] is None:
  					post.append('null')
  				else:
  					post.append(tmp['in_reply_to_status_id_str'].encode('utf-8'))
  				## repost_count,
  				post.append(tmp['repost_count'])
  				## in_reply_to_user_id varchar(128),
  				if tmp['in_reply_to_user_id'] is None:
  					post.append('null')
  				else:
  					post.append(tmp['in_reply_to_user_id'])
  				posts.append(post)
				#print posts
				#print reposts
				#break
				# print len(profile)
			except Exception, e:
					print e
			   		print 'exception:user %s'%user_id
			   		print tmp
			   		continue
	count = count + 1
	if count%50==0:
		print 'handle %s users\' posts.'%count
	
	## insert the post count info into mysql db
	post_counts = []
	for i in range(7):
		for j in range(24):
			tmp_count = [0 for k in range(6)]
			tmp_count[0] = user_id
			tmp_count[1] = i
			tmp_count[2] = j
			tmp_count[3] = total_count[i][j] # total number/4 weeks (we collect four weeks posts)
			tmp_count[4] = repost_count[i][j] # repost number
			tmp_count[5] = reply_count[i][j] # reply number
			post_counts.append(tmp_count)
	total_count = []
	repost_count = []
	reply_count = []
	#print post_counts
	#break
	try:
		cur.executemany('insert ignore into twitter_posts values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',posts)
		cur.executemany('insert ignore into twitter_reposts values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',reposts)
		#cur.executemany('insert ignore into post_count values(%s,%s,%s,%s,%s,%s)', post_counts)
		conn.commit()
		fi.write(user_id + '\n')
		fi.flush()
		posts = []
		reposts = []
		post_counts = []
	except MySQLdb.Error,e:
		print "Mysql Error %d: %s" % (e.args[0], e.args[1])
     	
	f.close()
	
fi.close()
try:
    cur.close()
    conn.close()
except MySQLdb.Error,e:
     print "Mysql Error %d: %s" % (e.args[0], e.args[1])
