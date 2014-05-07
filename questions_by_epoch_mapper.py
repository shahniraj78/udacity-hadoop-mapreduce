#! /usr/bin/python

import sys
import re
from datetime import datetime
from datetime import timedelta

reader = csv.reader(sys.__stdin__,delimiter="\t")

questions_epoch_dict = dict()

for data in reader:
	if data:# Skip the header row
		node_id = data[0].strip('"')
		if node_id.lower() != "id":
			epoch_key = None
		ques_dt = datetime.strptime(data[8].split("+")[0],"%Y-%m-%d %H:%M:%S.%f")
		curr_time = datetime.now()
		time_d = int((curr_time - ques_dt)/timedelta(day=1))
		
		if time_d <= 1:
			epoch_key = "in last day"
		elif time_d <= 7:
			epoch_key = "in past week"
			
		if epoch_key:
			for word in re.split(r'[\s\.\?\(\)\<\>\/\[\],!#=\-;\:\"\$]',data[4]):
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
							
if questions_epoch_dict():
	for tag_epoch, q_list in questions_epoch_dict.items():
		print "%s\t%s" %(tag_epoch,",".join(q_list))
