#! /usr/bin/python

#Description: This map reduce job combines forum posts and user data to add a fun feature to search that is based on forum topics and user reputations i.e. when searching for a given topic, data computed from this map reduce job will provide # of questions relating to the topic have been created by "Budding" (user reputation < 1000), "Rising Stars" (user reputation between 1000 & 5000), "Stars" (user reputation between 5000 & 15000) & "Superstars" (user reputation > 15000)  

# Requirements : This Map Reduce job requires higher memory allocation (1024 MB instead of Udacity/Cloudera training VM standard 512 MB) 

#MapReduce Design/Pseudo Logic
#Mapper iterates through forum and user data and outputs the following..
# 1) user data by labeling it with "user" label and computing the user's assumed reputation ("budding" vs "rising star" vs "star" vs "superstar") based on value from user data's reputation column 
# 2) forum question data with "post" label, question id, taglist (comma separated), user id
#Reducer collates the user and question data from mapper outputs and combines the topics from forum questions with reputations from user data and builds the list of questions

#Mapper Design
# 1) Read and Loop through all forum posts and user data
# 2) Identify the record type (user vs post) based on the header row and column
# 3) For user data, compute user's star reputation status based on value in user's reputation data column and write to mapper output   
# 4) For post data, add a "post" label identifier to question data, convert the space delimited tag list to comma delimited and write to mapper in the following format (tab delimited): "post" label, question id, taglist, user id

import sys
import csv

	#user_reader = csv.reader(user_file,delimiter="\t")
user_reader = csv.reader(sys.stdin,delimiter="\t")
record_type = None
for data in user_reader:
	if data[0] == "user_ptr_id":
		record_type = "user" 
	elif data[0]=="id":
		record_type = "post"

	if record_type == "user":
		user_id,rep,gold,silver,bronze = data
		if user_id != "user_ptr_id":
			rep = int(rep)
			if rep >= 15000:
				rep = "Superstar"
			elif 5000<= rep < 15000:
				rep = "Star"
			elif 1000 <= rep < 5000:
				rep = "Rising Star"
			else:
				rep = "Budding"
			print "%s\t%s\t%s" %(record_type,user_id,rep)
	else:
		if data[5] == "question":
			taglist = data[2]
			tags = taglist.replace(" ",",")
			#post,node id, comma separated tag list, user id
			print "%s\t%s\t%s\t%s" %(record_type,data[0],tags,data[3])
	
