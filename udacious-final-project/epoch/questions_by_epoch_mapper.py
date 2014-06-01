#! /usr/bin/python

#Description: Provides the timing dimension and summarization of questions relating to forum topics i.e. for a given tag/topic, computes the timeframe ( < 1 day : in last day, < 7 days : in pask week, > 7 days : old), total # of questions and list of questions falling in that timeframe 

#MapReduce Design/Pseudo Logic
#Mapper iterates through data, creates a dictionary that uses combination of tag and time as key and list of questions as the value
#Reducer creates a dictionary that collates outputs from mapper and publishes the tag, time, count of questions and list of questions

#Mapper Design
# 1) Read and Loop through all forum posts data
# 2) Create the epoch key based on timing of the forum post 
# 3) Split the list of tags from each forum posts 
# 4) build the dictionary that uses tag and epoch as key and list of questions as value 
# 5) Iterate through dictionary created in Step #4 and write to output in the following format : "tag:epoch	list of questions"


import sys
import csv
import re
from datetime import datetime
from datetime import timedelta

reader = csv.reader(sys.__stdin__,delimiter="\t")

questions_epoch_dict = dict()

for data in reader:
	if data:# Skip the header row
		node_id = data[0].strip('"')
		if node_id.lower() != "id" and data[5] == "question" :
			epoch_key = None
			ques_dt = datetime.strptime(data[8].split("+")[0],"%Y-%m-%d %H:%M:%S.%f")
			curr_time = datetime.now()
			
			time_d = curr_time - ques_dt
			time_d = int(time_d.days)
			
			if 0 <= time_d <= 1:
				epoch_key = "in last day"
			elif 1 < time_d <= 7:
				epoch_key = "in past week"
			else:
				epoch_key = "old"

			if epoch_key:
				tagslist = data[2]
				if tagslist:
					tags = tagslist.split(" ")
					if tags:
						for word in tags:
							if not word == "":
								lower_word = word.lower()
								dict_key = "%s:%s" %(lower_word,epoch_key)
								if questions_epoch_dict.has_key(dict_key):
									node_list = questions_epoch_dict[dict_key]
									if not node_id in node_list:
										node_list.append(node_id)
										questions_epoch_dict[dict_key] = node_list
								else:
									questions_epoch_dict[dict_key]  = [node_id]
									
if questions_epoch_dict:
	#print questions_epoch_dict
	for tag_epoch, q_list in questions_epoch_dict.items():
		print "%s\t%s" %(tag_epoch,",".join(q_list))
