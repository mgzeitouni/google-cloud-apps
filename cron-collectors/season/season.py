class Season():
	
	def __init__(self, sportName, year_start, year_end, sportId = None, conn = None, cursor = None, ):

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


