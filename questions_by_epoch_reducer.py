#! /usr/bin/python

import sys
import csv
import re
from datetime import datetime
from datetime import timedelta

reader = csv.reader(sys.__stdin__,delimiter="\t")

questions_epoch_dict = dict()

for data in reader:
	if data:
		
