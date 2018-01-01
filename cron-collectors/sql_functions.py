import mysql.connector
from mysql.connector import Error
from team.team import Team
from sport.sport import Sport
from season.season import Season
from event.event import Event
import datetime
from datetime import timedelta
import csv
import dateutil.parser as dparser
import pdb

def connect():
    """ Connect to MySQL database """
    try:
        conn = mysql.connector.connect(host='35.196.174.154',
                                       database='KarteesMasterData',
                                       user='root',
                                       password='kartees12')
        if conn.is_connected():
            print('Connected to MySQL database')
        
 
    except Error as e:
        print(e)
        cursor = None
        conn = None

    return conn



def insert_all_events(conn, sportName, seasonName, fileName):

  with open('game_schedules/%s/%s.csv' %(sportName, fileName) ,'rU') as f:

    reader = csv.reader(f)
    reader.next()

    for row in reader:

      event = Event(conn=conn, stubhubId=row[3], sportName=sportName, seasonName=seasonName,  teamId = row[0], teamCity=row[1], teamName=row[2], dateUTC=dparser.parse(row[4]), dateLocal=dparser.parse(row[5]))

      try:
        event.insert_event(conn)
      except:
        print ("Error with inserting event %s - could be duplicate" %row[3])

def get_all_teams(sport):

  conn = connect()

  cursor = conn.cursor(buffered=True)
  
  cursor.execute("SELECT Id FROM `Sport` WHERE `name`='%s'" % (sport))

  sportId = cursor.fetchall()[0][0]

  cursor.execute("SELECT * FROM `Team` WHERE `sportId`='%s'" %(sportId))

  teams = cursor.fetchall()

  teams_objects = []

  for team in teams:
    teams_objects.append(Team(team[0],team[1],team[2],sport))

  conn.close()

  return teams_objects

def get_zero_seven_events(sportName, season_year_start, season_year_end):

  conn = connect()

  season = Season(conn=conn, sportName = sportName, year_start= season_year_start, year_end=season_year_end)

  cursor = conn.cursor(buffered=True)

  query = ("SELECT * FROM `Event` WHERE `seasonId` = '%s' AND `dateUTC` BETWEEN %s AND %s ORDER BY dateUTC ASC")
  cursor.execute(query, (season.seasonId, datetime.datetime.utcnow(),datetime.datetime.utcnow()+timedelta(days=7)))

  data = cursor.fetchall()

  events = []
  if len(data)>0:
    for i in data:
      events.append(Event(conn=conn, stubhubId=i[0], dateUTC=i[1],sportName=sportName, seasonName=season.name, teamId=i[2]))

  return events


def get_seven_fifteen_events(sportName, season_year_start, season_year_end):

  conn = connect() 
  
  season = Season(conn=conn, sportName = sportName, year_start= season_year_start, year_end=season_year_end)

  cursor = conn.cursor(buffered=True)

  query = ("SELECT * FROM `Event` WHERE `seasonId` = '%s' AND `dateUTC` BETWEEN %s AND %s ORDER BY dateUTC ASC")
  cursor.execute(query, (season.seasonId, datetime.datetime.utcnow()+timedelta(days=7),datetime.datetime.utcnow()+timedelta(days=15)))

  data = cursor.fetchall()

  events = []
  if len(data)>0:
    for i in data:
      events.append(Event(conn=conn, stubhubId=i[0], dateUTC=i[1],sportName=sportName, seasonName=season.name, teamId=i[2]))

  return events

def get_fifteen_up_to_collect(sportName, season_year_start, season_year_end, hour, minute):

  conn = connect() 

  season = Season(conn=conn, sportName = sportName, year_start= season_year_start, year_end=season_year_end)

  cursor = conn.cursor(buffered=True)

  # First get team for this time slot
  query = ("SELECT teamId FROM `Cron_Schedule` WHERE `seasonId`='%s' AND `time_hour` = '%s' AND `time_minute`= '%s'")
  cursor.execute(query, (season.seasonId, hour,minute))

  data = cursor.fetchall()

  # If there is a team for this time, get their events that are 15 + days away
  events = []
  if len(data)>0:
    teamId = data[0][0]

    query = ("SELECT * FROM `Event` WHERE teamId = '%s' AND `dateUTC` BETWEEN %s AND %s ORDER BY dateUTC ASC")
    cursor.execute(query, (teamId, datetime.datetime.utcnow()+timedelta(days=15),datetime.datetime.utcnow()+timedelta(days=250) ))

    data = cursor.fetchall()

    for i in data:
      events.append(Event(conn=conn, stubhubId=i[0], dateUTC=i[1],sportName=sportName, seasonName=season.name, teamId=teamId))

  return events






if __name__ == '__main__':

    conn = connect()

    #insert_all_events(conn=conn, sportName='NHL', seasonName="NHL_2017-18", fileName='2017-12-13_19_13')

    conn.close()
