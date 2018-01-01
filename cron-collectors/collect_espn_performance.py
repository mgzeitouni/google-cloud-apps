import datetime
import pdb
import argparse
import logging
from sql_functions import *
import time
from publish_message import *
import requests
import json

def call_API_for_data(team,sport, logger):

	base_url = "https://stubhub-services-python.mybluemix.net/get_team_performance?"

	params = "team=%s %s&sport=%s" %(team.city, team.name,sport.lower())
	
	full = base_url+params

	response = requests.get(full)

	try:
		data = json.loads(response.text)
		
		# Add team for the storage
		data['team'] = "%s %s" %(team.city, team.name)
		data['sport'] = sport
		
		if response.status_code != 200:
			logger.info("Error with collecting ESPN performance %s - %s" %(team.city, team.name))
			return None
		else:
			return data
	except:
		logger.info ("Error loading %s"%(team.city, team.name))
		return None


def collect_performance(sport, logger):

	teams = get_all_teams(sport)

	for team in teams:

		data = call_API_for_data(team,sport, logger)
		
		# If good, send to Google PubSub
		try:
			topic_name = 'espn_performance'
			logger.info(publish_message(topic_name, data, data['sport'], data['team']))
		except:
			print ("Error")



if __name__ == '__main__':

	parser = argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
	parser.add_argument('sport', help='The name of the sport')

	args = parser.parse_args()
	
	logger = logging.getLogger('espn_performance_%s'%args.sport)
	logger.setLevel(logging.DEBUG)

	# create a file handler writing to a file named after the thread
	file_handler = logging.FileHandler('espn_performance_%s.log' % (args.sport))

	# create a custom formatter and register it for the file handler
	formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s",
                          "%Y-%m-%d %H:%M:%S")
	file_handler.setFormatter(formatter)

	# register the file handler for the thread-specific logger
	logger.addHandler(file_handler)


	collect_performance(args.sport, logger)
