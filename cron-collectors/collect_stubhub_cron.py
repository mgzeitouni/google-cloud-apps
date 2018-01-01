import datetime
import pdb
import argparse
from sql_functions import *
import threading
import logging
import time
from publish_message import *
import requests
import json
import multiprocessing

def call_API_for_data(dataType, path, account, event, sport, logger):

	base_url = "https://stubhub-services-python.mybluemix.net/get_event_data?"

	params = "dataType=%s&account=%s&eventId=%s" %(dataType,account,event.stubhubId)
	
	response = requests.get(base_url+params)

	data = json.loads(response.text)


	# Add team for the storage
	data['team'] = "%s %s" %(event.team.city, event.team.name)
	data['sport'] = sport
	data['eventId'] = event.stubhubId 
	# Check if successful response
	if response.status_code != 200 or int(data['success']) != 1:
		logger.info("Error with collecting event %s - %s" %(event.stubhubId,dataType))
		return None
	else:
		return data

def collect_data(i, events, sport, account, collection_type, logger):

	logger.info ("Games %s New data collection request: %s" %(collection_type, datetime.datetime.utcnow()))

	# Loop through each event to collect data
	for event in events:
		# try:
		logger.info("Running event: %s" %event.stubhubId)

		# Wait 7 seconds to ensure we don't go over API limits
		time.sleep(7)

		# First collect inventory data
		dataType = "inventory"
		path = "event_inventory"
		data = call_API_for_data(dataType, path, account, event, sport, logger)
		
		# If good, send to Google PubSub
		try:
			topic_name = 'raw_stubhub_%s' %path
			logger.info(publish_message(topic_name, data, data['sport'], data['team']))
		except:
			print ("Error")

		# Then metadata
		dataType = "meta"
		path = "event_metadata"
		data = call_API_for_data(dataType, path, account, event, sport, logger)

		# If good, send to Google PubSub
		try:
			topic_name = 'raw_stubhub_%s' %path
			logger.info(publish_message(topic_name, data, data['sport'], data['team']))
		except:
			print ("Error")



	logger.info("Done with all events")
	f=open("done%s_%s.txt"%(i,sport),"w+")
	f.write("1")
	f.close()


def events_to_collect(sport, season_year_start, season_year_end, account15up, account015):
	
	# Open Done Flag file to see if last process finished or not
	try:
		done_thread_0 = int(open("done0_%s.txt"%sport,"rU").read())
		done_thread_1 = int(open("done1_%s.txt"%sport,"rU").read())
	except:
		done_thread_0=1
		done_thread_1=1

	# Get current time
	minute = int(datetime.datetime.utcnow().minute)
	hour = int(datetime.datetime.utcnow().hour % 6)

	print ("%s: Hour-Minute: %s-%s" %(datetime.datetime.utcnow(), hour, minute))

	# Check if previous tasks are done with, or else don't start this one
	if done_thread_0 != 1:
		print("%s: Previous 15 & UP task not finished" %datetime.datetime.utcnow())
		thread_0 = [] 

	else:
		print("%s: Querying for 15 & up events..." %datetime.datetime.utcnow())
		thread_0 = get_fifteen_up_to_collect(sport, season_year_start, season_year_end, hour, minute)
		print ("%s: Got %s events for 15 & up!" %( datetime.datetime.utcnow(), len(thread_0)))
	thread_1 = []

	if done_thread_1 == 1:

		# If we're on even hour - collect the seven to fifteen day events
		if hour % 2 == 0 and minute == 0:
			print ("%s: Querying for 7 - 15 events..." % datetime.datetime.utcnow())
			thread_1 = get_seven_fifteen_events(sport, season_year_start, season_year_end)
			print ("%s: Got %s events for 7 - 15!" %(datetime.datetime.utcnow(),len(thread_1)))
			collection_type = "7 - 15"
		# Else, collect zero to seven day events
		else:
			print ("Querying for 0 - 7 events...")
			thread_1 = get_zero_seven_events(sport, season_year_start, season_year_end)
			print ("Got %s events for 0 - 7!" %len(thread_1))
			collection_type = "0 - 7"

	else:
		print("%s: Previous 0 - 15 task not finished" % datetime.datetime.utcnow())


	for i in range(2):

		logger = logging.getLogger('thread-%s_%s' % (i,sport))
		logger.setLevel(logging.DEBUG)

		# create a file handler writing to a file named after the thread
		file_handler = logging.FileHandler('thread-%s_%s.log' % (i,sport))

		# create a custom formatter and register it for the file handler
		formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s",
                              "%Y-%m-%d %H:%M:%S")
		file_handler.setFormatter(formatter)

		# register the file handler for the thread-specific logger
		logger.addHandler(file_handler)


		if i==0:
			events = thread_0
			account = account15up
			collection_type = "15 & Up"
			collect = True if done_thread_0 == 1 else False


		else:
			events = thread_1
			account = account015
			collection_type = "0 - 7"
			collect = True if done_thread_1 == 1 else False

		if collect:
			f=open("done%s_%s.txt"%(i,sport),"w+")
			f.write("0")
			f.close()
			t = threading.Thread(target=collect_data, args=(i, events, sport, account, collection_type, logger,))
			t.start()
		
		

if __name__ == '__main__':


	parser = argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
	parser.add_argument('sport', help='The name of the sport')
	parser.add_argument('season_year_start', help='year season starts')
	parser.add_argument('season_year_end', help='year season ends')
	parser.add_argument('account15Up', help='Account to use for games 15 days away')
	parser.add_argument('account015', help='Account to use for games 0-15 days away')

	args = parser.parse_args()

	# Start foo as a process
	p = multiprocessing.Process(target=events_to_collect, name="Foo", args=(args.sport, args.season_year_start, args.season_year_end, args.account15Up, args.account015,))
	p.start()

	# Wait a maximum of 19 minutes seconds for foo
	# Usage: join([timeout in seconds])
	p.join(60*19)

	# If thread is active
	if p.is_alive():
	    print ("foo is running... let's kill it...")

	    # Terminate foo
	    p.terminate()
	    p.join()


	# events_to_collect(args.sport, args.season_year_start, args.season_year_end, args.account15Up, args.account015)


