import datetime
import pdb
import argparse
from sql_functions import *

def collect_data(sport, season_year_start, season_year_end, account15up, account07):

	# Evaluate current time

	f = open('done_flag.txt', 'rU')
	done = eval(f.read())

	minute = datetime.datetime.utcnow().minute
	hour = datetime.datetime.utcnow().hour

	zero_seven_to_collect = []
	seven_fifteen_to_collect = []
	fifteen_up_to_collect = []

	if minute % 2 == 1:

		if done==True:

			zero_seven_to_collect = get_zero_seven_events()


	elif hour % 2 == 0 and minute == 0:
		print 'middle'

	else:
		print 'last'




if __name__ == '__main__':

	parser = argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
	parser.add_argument('sport', help='The name of the sport')
	parser.add_argument('season_year_start', help='year season starts')
	parser.add_argument('season_year_end', help='year season ends')
	parser.add_argument('account15Up', help='Account to use for games 15 days away')
	parser.add_argument('account07', help='Account to use for games 0-7 days away')

	args = parser.parse_args()

	collect_data(args.sport, args.season_year_start, args.season_year_end, args.account15Up, args.account07)


