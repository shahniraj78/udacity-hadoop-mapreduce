#! /usr/bin/python

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
		print "%s\t%s\t%s\t%s" %(tag,epoch,len(q_list),",".join(q_list))
