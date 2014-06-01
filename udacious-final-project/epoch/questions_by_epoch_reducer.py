#! /usr/bin/python

#Description: Provides the timing dimension and summarization of questions relating to forum topics i.e. for a given tag/topic, computes the timeframe ( < 1 day : in last day, < 7 days : in pask week, > 7 days : old), total # of questions and list of questions falling in that timeframe 

#MapReduce Design/Pseudo Logic
#Mapper iterates through data, creates a dictionary that uses combination of tag and time as key and list of questions as the value
#Reducer creates a dictionary that collates outputs from mapper and publishes the tag, time, count of questions and list of questions

#Reducer Design
# 1) Read and Loop through all mapper output
# 2) Build a dictionary that collates all the mapper output to get a complete list of tags, epoch and list of questions 
# 4) Iterate through dictionary created in Step #2 and write to output for each tag and epoch, total # of questions and list of questions

import sys
import csv
import re
from datetime import datetime
from datetime import timedelta

questions_epoch_dict = dict()

for line in sys.stdin:
	data = line.strip().split("\t")
	if data:
		if len(data) == 2:
			tag_epoch_key, q_list = data 
			if questions_epoch_dict.has_key(tag_epoch_key):
				tag_epoch_qlist = questions_epoch_dict[tag_epoch_key]
				if tag_epoch_key in tag_epoch_qlist:
					tag_epoch_qlist.append(q_list)
					questions_epoch_dict[tag_epoch_key] = tag_epoch_qlist
			else:
				questions_epoch_dict[tag_epoch_key] = [q_list]

if questions_epoch_dict:
	for tag_epoch_key,q_list in questions_epoch_dict.items():
		tag,epoch = tag_epoch_key.split(":")
		q_list_str = ",".join(q_list)
		print "%s\t%s\t%s\t%s" %(tag,epoch,len(q_list_str.split(",")),q_list_str)

