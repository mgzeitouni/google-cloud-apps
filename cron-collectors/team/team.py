class Team():
	
	def __init__(self, stubhubId, city, name, sportId = None, conn = None, sportName=None):

		self.stubhubId = stubhubId

		self.city = city

		self.name = name

		self.sportId = sportId

		if conn:

			cursor = conn.cursor(buffered=True)

			sportId ="SELECT * FROM Sport WHERE name='%s'" % (sportName,)

			cursor.execute("SELECT Id FROM `Sport` WHERE `name`='%s'" % (sportName))

			data = cursor.fetchall()

			self.sportId = data[0][0]

			
