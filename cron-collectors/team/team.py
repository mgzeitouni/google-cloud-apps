class Team():
	
	def __init__(self, stubhubId, city, name, sportName, sportId = None, conn = None):

		self.stubhubId = stubhubId

		self.city = city

		self.name = name

		self.sportId = sportId

		if conn:

			cursor = conn.cursor(buffered=True)

			cursor.execute("SELECT Id FROM `Sport` WHERE `name`='%s'" % (sportName))

			data = cursor.fetchall()

			self.sportId = data[0][0]

	def insert_team(self, conn):

		if self.sportId:

			add_team = ("INSERT INTO Team "
               "(stubhubId, city, name, sportid) "
               "VALUES (%(stubhubId)s, %(city)s, %(name)s, %(sportId)s)" )

			team_data = {"stubhubId":self.stubhubId,"name":self.name, "city": self.city, "sportId":self.sportId}

			cursor = conn.cursor(buffered=True)

			cursor.execute(add_team,team_data)

			print ("Adding team %s %s" %(self.city, self.name))

			conn.commit()	
		
		else:

			print ("Error, no sportId defined")


			
