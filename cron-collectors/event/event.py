import sys
sys.path.append("..") 
from team.team import Team

class Event():
	
	def __init__(self, stubhubId, dateUTC,sportName, seasonName, teamCity, teamName,teamId=None, dateLocal=None, conn = None  ):

		self.stubhubId = stubhubId
		self.dateUTC = dateUTC
		self.dateLocal = dateLocal

		self.teamId = teamId
		self.teamName = teamName
		self.teamCity = teamCity
		self.sportName = sportName

		if conn:

			cursor = conn.cursor(buffered=True)

			cursor.execute("SELECT Id FROM `Sport` WHERE `name`='%s'" % (sportName))

			data = cursor.fetchall()

			self.sportId = data[0][0]

			
			# Find if team exists,  
			cursor.execute("SELECT stubhubId FROM `Team` WHERE `stubhubId`='%s'" % (self.teamId))

			data = cursor.fetchall()

			if len(data) >0:

				self.teamId = data[0][0]
				self.team_exists = True

			else:
				self.team_exists = False



			cursor.execute("SELECT Id FROM `Season` WHERE `name`='%s'" % (seasonName))

			data = cursor.fetchall()

			self.seasonId = data[0][0]

	def insert_event(self, conn):

		cursor = conn.cursor(buffered=True)

		if self.sportId:
			if self.seasonId:

				if not self.team_exists:

					print 'No team found - creating %s %s' % (self.teamCity, self.teamName)
					team = Team(stubhubId=self.teamId, city=self.teamCity, name=self.teamName, sportName=self.sportName, conn = conn)

					team.insert_team(conn)

				cursor = conn.cursor(buffered=True)

				cursor.execute("SELECT city, name FROM `Team` WHERE `stubhubId`='%s'" % (self.teamId))

				data = cursor.fetchall()

				if len(data)>0:
					city, name = data[0][0], data[0][1]
				else:
					print ("Team ")


				# Insert event
				add_event = ("INSERT INTO Event "
           					"(stubhubId, dateUTC, teamId, sportId, seasonId, dateLocal) "
           					"VALUES (%(stubhubId)s, %(dateUTC)s, %(teamId)s, %(sportId)s, %(seasonId)s, %(dateLocal)s)" )

  				event_data = {"stubhubId":self.stubhubId,"dateUTC":self.dateUTC, "teamId":self.teamId, "sportId":self.sportId,"seasonId":self.seasonId, "dateLocal":self.dateLocal }

				

				cursor.execute(add_event, event_data)

				# Get city and name of team to print
				cursor.execute("SELECT city, name FROM `Team` WHERE `stubhubId`='%s'" % (self.teamId))

				data = cursor.fetchall()

				city, name = data[0][0], data[0][1]

				print ("Adding Event %s for team: %s %s" %(self.stubhubId, city, name))

				conn.commit()


			else:
				print ("Error, seasonId not defined")
		else:
			print ("Error, sportId not defined")







