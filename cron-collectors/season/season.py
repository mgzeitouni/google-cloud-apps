class Season():
	
	def __init__(self, sportName, year_start, year_end, sportId = None, conn = None ):

		self.year_start = year_start
		self.year_end = year_end
		self.name = sportName + "_" + str(year_start)

		if year_start!=year_end:
			self.name+="-"+ str(year_end)[2:4]
		
		if conn:

			cursor = conn.cursor(buffered=True)

			sportId ="SELECT * FROM Sport WHERE name='%s'" % (sportName,)

			cursor.execute("SELECT Id FROM `Sport` WHERE `name`='%s'" % (sportName))

			data = cursor.fetchall()

			self.sportId = data[0][0]


	def insert_season(self, conn):

		if self.sportId:

			add_season = ("INSERT INTO Season "
		               "(name, sportId, year_start, year_end) "
		               "VALUES (%(name)s, %(sportId)s, %(year_start)s, %(year_end)s )" )

		  	season_data = {"name":self.name, "sportId":self.sportId, "year_start":self.year_start, "year_end":self.year_end}

		  	cursor = conn.cursor(buffered=True)
		  
		  	cursor.execute(add_season,season_data)

		  	print ("Adding season with Name: %s" %self.name)

		  	conn.commit()

		  	cursor.close()

		else:

			print ("Error, no sportId defined")


