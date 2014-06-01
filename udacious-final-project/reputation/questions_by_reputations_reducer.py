#! /usr/bin/python

#Description: This map reduce job combines forum posts and user data to add a fun feature to search that is based on forum topics and user reputations i.e. when searching for a given topic, data computed from this map reduce job will provide # of questions relating to the topic have been created by "Budding" (user reputation < 1000), "Rising Stars" (user reputation between 1000 & 5000), "Stars" (user reputation between 5000 & 15000) & "Superstars" (user reputation > 15000)  

# Requirements : This Map Reduce job requires higher memory allocation (1024 MB instead of Udacity/Cloudera training VM standard 512 MB) 

#MapReduce Design/Pseudo Logic
#Mapper iterates through forum and user data and outputs the following..
# 1) user data by labeling it with "user" label and computing the user's assumed reputation ("budding" vs "rising star" vs "star" vs "superstar") based on value from user data's reputation column 
# 2) forum question data with "post" label, question id, taglist (comma separated), user id
#Reducer collates the user and question data from mapper outputs and combines the topics from forum questions with reputations from user data and builds the list of questions

#Reducer Design
# 1) Read each line of mapper output from stdin
# 2) Identify the record type (user vs post) based on the "user" or "post" label added by mapper for each row
# 3) Create a dictionary for user data with user_id as key and star reputation as the lookup value  
# 4) For post data, create a dictionary with "tag" and "user_id" combination as key and question list as the lookup value
# 5) Combine the data from post and user dictionaries created in Steps 3 & 4 to create a dictionary of tag, reputation and list of questions
#6) Write the tag, reputation, # of questions and the question list to reducer output

import sys

questions_by_reputation = dict()

user_reputation_dict = dict()
tag_user_dict = dict()
tag_reputation_qlist = dict()

for line in sys.stdin:
	data = line.strip().split("\t")
	
	if data[0] == "user":
		user,user_id,rep = data
		user_reputation_dict[user_id] = rep
	else:
		post,node_id,taglist,user_id = data
		if taglist:
			tags = taglist.split(",")
			if tags:
				for word in tags:
					if not word == "":
						lower_word = word.lower()
						dict_key = "%s:%s" %(lower_word,user_id)
						#dict_key = lower_word
						if tag_user_dict.has_key(dict_key):
							qlist = tag_user_dict[dict_key]
							qlist.append(node_id)
							tag_user_dict[dict_key] = qlist
						else:
							tag_user_dict[dict_key] = [node_id]

if tag_user_dict:
	for word_user_id,ql in tag_user_dict.items():
		word, user_id = word_user_id.split(":")
		dict_key = "%s:%s" %(word,user_reputation_dict[user_id])
		if tag_reputation_qlist.has_key(dict_key):
			tag_reputation_qlist[dict_key] = tag_reputation_qlist[dict_key] + ql
		else:
			tag_reputation_qlist[dict_key] = ql


user_reputation_dict = None
tag_user_dict = None

if tag_reputation_qlist:
	for tag_rep_key,q_list in sorted(tag_reputation_qlist.items()):
		tag,rep = tag_rep_key.split(":")
		q_list_str = ",".join(q_list)
		print "%s\t%s\t%s\t%s" %(tag,rep,len(q_list_str.split(",")),q_list_str)
