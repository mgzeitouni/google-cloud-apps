import datetime
import pdb
import argparse
from sql_functions import *

def insert_crons(conn, sport, seasonName):

	cursor = conn.cursor(buffered=True)

	# Query for all teams

	teams = get_all_teams(sport)

	hour = 0
	minute = 0

	cursor.execute("SELECT Id FROM `Season` WHERE `name`='%s'" % (seasonName))

	seasonId = cursor.fetchall()[0][0]

	for team in teams:	

		# Insert into Corn Schedule table
		add_cron = ("INSERT INTO Cron_Schedule "
		"(time_hour, time_minute, seasonId, teamId) "
		"VALUES (%(time_hour)s, %(time_minute)s, %(seasonId)s, %(teamId)s)" )

		cron_data = {"time_hour":hour,"time_minute":minute, "seasonId": seasonId, "teamId":team[0]}

		cursor = conn.cursor(buffered=True)

		cursor.execute(add_cron,cron_data)

		conn.commit()

		if minute==50:
			minute=0
			hour+=1
		else:
			minute+=10


if __name__ == '__main__':

	parser = argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
	parser.add_argument('sport', help='The name of the sport')
	parser.add_argument('seasonName', help='Season Name')

	args = parser.parse_args()

	conn = connect()

	insert_crons(conn, args.sport, args.seasonName)


