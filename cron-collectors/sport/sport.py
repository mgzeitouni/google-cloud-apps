class Sport():
	
	def __init__(self,id, name):

		self.name =name
		self.id=id

	def insert_sport(self, conn):

		add_sport = ("INSERT INTO Sport "
               "(id, name) "
               "VALUES (%(Id)s, %(name)s)" )

		sport_data = {"Id":sport.id,"name":sport.name}

		cursor = conn.cursor(buffered=True)

		cursor.execute(add_sport,sport_data)

		print ("Adding sport %s with Id: %s" %(self.name, self.id))

		conn.commit()

		cursor.close()

