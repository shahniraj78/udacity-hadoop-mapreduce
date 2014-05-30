#! /usr/bin/python

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
		post,node_id,taglist,user_id,node_type = data
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
