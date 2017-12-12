import os

import MySQLdb
import webapp2

def connect():
    db = MySQLdb.connect(
            unix_socket=cloudsql_unix_socket,
            user=CLOUDSQL_USER,
            passwd=CLOUDSQL_PASSWORD)

 
 
if __name__ == '__main__':
    connect()