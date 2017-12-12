import mysql.connector
from mysql.connector import Error
from team.team import Team
from sport.sport import Sport
from season.season import Season
 
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


def insert_sport(conn, id, name):

  sport = Sport(name=name,id=id)

  add_sport = ("INSERT INTO Sport "
               "(id, name) "
               "VALUES (%(Id)s, %(name)s)" )

  sport_data = {"Id":sport.id,"name":sport.name}

  cursor = conn.cursor(buffered=True)

  cursor.execute(add_sport,sport_data)

  conn.commit()

  cursor.close()


def insert_team( conn, stubhubId, name, city, sportName=None ):

  team = Team(stubhubId = stubhubId, name = name, city=city, sportName =sportName, conn=conn)
  
  add_team = ("INSERT INTO Team "
               "(stubhubId, city, name, sportid) "
               "VALUES (%(stubhubId)s, %(city)s, %(name)s, %(sportId)s)" )

  team_data = {"stubhubId":team.stubhubId,"name":team.name, "city": team.city, "sportId":team.sportId}

  cursor = conn.cursor(buffered=True)

  cursor.execute(add_team,team_data)

  conn.commit()

  cursor.close()

def insert_season(conn, sportName, year_start, year_end ):

  season = Season(conn = conn, sportName=sportName, year_start = year_start, year_end=year_end)
  
  add_season = ("INSERT INTO Season "
               "(name, sportId, year_start, year_end) "
               "VALUES (%(name)s, %(sportId)s, %(year_start)s, %(year_end)s )" )

  season_data = {"name":season.name, "sportId":season.sportId, "year_start":season.year_start, "year_end":season.year_end}

  cursor = conn.cursor(buffered=True)
  
  cursor.execute(add_season,season_data)

  conn.commit()

  cursor.close()



 
if __name__ == '__main__':

    conn = connect()

    #insert_team(stubhubId = 12, name='mo', city='ny', sportName='MLB', conn=conn )

    insert_season(conn, sportName="NHL", year_start = 2017, year_end = 2018 )

    conn.close()
