#! /usr/bin/python

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
			print "%s\t%s\t%s\t%s\t%s" %(record_type,data[0],tags,data[3],data[5])
	
