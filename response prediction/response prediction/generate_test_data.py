#! /usr/bin/python
# -*- coding: utf-8 -*-

import os, csv
import MySQLdb

# generate the scope of user's ids that's need to collect
# [strat ---- end)
def generateTrainingDataset(start, end):	
	user_posts = load_userposts(start, end)
	timeSeries = load_timeSeries()
	user_infos = load_userInfos()
	links = load_links()
	timeDistances = load_timeDistances()
	topic_sim = load_topicsim()
	ratios = load_ratios()
	responses = load_responses()
	
	count = 0
	out_file = open('./dataset/ds-1.csv', 'wb')
	writer = csv.writer(out_file)
	writer.writerow(['FON_v','FRN_v','ROFF_v','LT_v','FT_v','TN_v','FON_u','FRN_u','ROFF_u','LT_u','FT_u','TN_u','A_v','A_u','CA_uv','AS_uv','POT_uv','TS_uv','isReposted'])
	for user_id,posts in user_posts.items():
		#print 'user_id:%s'%user_id
		for post in posts:
			post_id = post[0]
			hour = post[1]
			#print 'post_id:%s,  hour:%s'%(post_id,hour)
			out_ids = links[user_id]
			for out_id in out_ids:
				#print 'out_id:%s'%out_id
				data = []
				data.extend(user_infos[user_id])
				#print 'user_info:'
				#print user_info
				data.extend(user_infos[out_id])
				#print 'out_info:'
				prob_user = float(timeSeries[user_id][int(hour)])
				prob_out = float(timeSeries[out_id][int(hour)])
				mix = prob_user*prob_out
				data.append(str('%.4f'%prob_user))
				data.append(str('%.4f'%prob_out))
				data.append(str('%.4f'%mix))
				data.append(timeDistances[user_id + '_' + out_id])
				#print data
				data.append(ratios[user_id + '_' + out_id])
				#print data
				data.append(topic_sim[user_id + '_' + out_id])
				# label the result
				label = user_id + '_' + out_id + '_' + post_id
				if label in responses:
					data.append('1')
				else:
					data.append('0')
    			
				writer.writerow(data)
				del data[:]
				count += 1
    			
				if count%100000 == 0:
					file_split = count/100000 + 1
					out_file.close()
					out_file = open('./dataset/ds-' + str(file_split) + '.csv', 'wb')
					writer = csv.writer(out_file)
					writer.writerow(['FON_v','FRN_v','ROFF_v','LT_v','FT_v','TN_v','FON_u','FRN_u','ROFF_u','LT_u','FT_u','TN_u','A_v','A_u','CA_uv','AS_uv','POT_uv','TS_uv','isReposted'])
				if count%100000 == 0:
					print 'already generate %s data records.'%count
	out_file.close()
	print 'done: %s repost between users ...'%count

# load posts that want to generate training dataset.
# user_id - [post_id,hour]
def load_userposts(start, end):	
	file = open('user_posts.dat', 'r')
	user_posts = {} #user_id - [post_id,hour]
	count = 0
	for line in file:
		if line == '':
			continue		
		items = line.split()
		if end != -1:
			if count < start or count >= end:
				continue
		user_id = items[0]
		post = []
		post.append(items[1])
		post.append(items[2])
		if user_id not in user_posts:
			posts = []
			posts.append(post)
			user_posts[user_id] = posts
		else:
			user_posts[user_id].append(post)
		count += 1
	print 'load %s user-posts.'%count
	file.close()
	return user_posts

# load user's time series
# user_id - [0 1 2 ... 23]
def load_timeSeries():
	file = open('timeSeries.dat', 'r')
	timeSeries = {} #user_id - [0 1 2 ... 23]
	count = 0
	for line in file:
		if line == '':
			continue
		count+=1
		items = line.split()
		user_id = items[0]
		timeSeries[user_id] = items[1:25]
	print 'load %s time series.'%count
	file.close()
	return timeSeries

def load_userInfos():
	file = open('user_info.dat', 'r')
	user_infos = {} #user_id - features
	count = 0
	for line in file:
		if line == '':
			continue
		items = line.split()
		user_infos[items[0]] = items[1:7]
		count+=1
	print 'load %s user-infos.'%count
	file.close()
	return user_infos
	
def load_links():
	file = open('links.dat', 'r')
	links = {} #user_id - features
	count = 0
	for line in file:
		if line == '':
			continue
		items = line.split()
		user_id = items[0]
		if user_id not in links:
			out_ids = []
			out_ids.append(items[1])
			links[user_id] = out_ids
		else:
			links[user_id].append(items[1])
		count+=1
	print 'load %s links.'%count
	file.close()
	return links

def load_timeDistances():
	file = open('i-j-cosine.dat', 'r')
	timeDistances = {}
	count = 0
	for line in file:
		if line == '':
			continue
		items = line.split()
		timeDistances[items[0] + '_' + items[1]] = items[2]
		count+=1
	print 'load %s timeDistances.'%count
	file.close()
	return timeDistances

def load_ratios():
	file = open('i-j-ratio.dat', 'r')
	ratios = {}
	count = 0
	for line in file:
		if line == '':
			continue
		items = line.split()
		ratios[items[0] + '_' + items[1]] = items[2]
		count+=1
	print 'load %s ratios.'%count
	file.close()
	return ratios
	
def load_topicsim():
	file = open('cosine-sim.dat', 'r')
	ts = {}
	count = 0
	for line in file:
		if line == '':
			continue
		items = line.split()
		ts[items[0]] = items[1]
		count+=1
	print 'load %s topic similarity.'%count
	file.close()
	return ts
	
def load_responses():
	file = open('reposts.dat', 'r')
	responses = {}
	count = 0
	for line in file:
		if line == '':
			continue
		items = line.split()
		responses[items[1] + '_' + items[0] + '_' + items[3]] = '1'
		count+=1
	print 'load %s reposts.'%count
	file.close()
	
	file = open('replys.dat', 'r')
	count = 0
	for line in file:
		if line == '':
			continue
		items = line.split()
		responses[items[1] + '_' + items[0] + '_' + items[3]] = '1'
		count+=1
	print 'load %s replys.'%count
	file.close()
	return responses
	
if __name__ == '__main__':
	start = 0
	end = -1
	generateTrainingDataset(start,end)# start --- end is the amount controller. It will collect integeral dataset when end = -1
	print 'all done!'
