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

  conn.close()

  return teams

def get_zero_seven_events():

  conn = connect() 

  cursor = conn.cursor(buffered=True)

  query = ("SELECT * FROM `Event` WHERE `dateUTC` BETWEEN %s AND %s")
  cursor.execute(query, (datetime.datetime.utcnow(),datetime.datetime.utcnow()+timedelta(days=7)))

  return cursor.fetchall()


def get_seven_fifteen_events():

  conn = connect() 

  cursor = conn.cursor(buffered=True)

  query = ("SELECT * FROM `Event` WHERE `dateUTC` BETWEEN %s AND %s")
  cursor.execute(query, (datetime.datetime.utcnow()+timedelta(days=7),datetime.datetime.utcnow()+timedelta(days=15)))

  return cursor.fetchall()





if __name__ == '__main__':

    conn = connect()

    #insert_all_events(conn=conn, sportName='NHL', seasonName="NHL_2017-18", fileName='2017-12-13_19_13')

    conn.close()
