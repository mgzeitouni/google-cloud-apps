from google.cloud import pubsub
import requests
from upload_object import *
import os
import json
from datetime import *
import time
import argparse



def open_subscription(sub_name, datatype):
	subscriber = pubsub.SubscriberClient()
	sub_name = 'projects/kartees-188316/subscriptions/%s' %sub_name

	subscription = subscriber.subscribe(
	    sub_name,
	)

	bucket = 'kartees-raw-data'

	# Define the callback.
	# Note that the callback is defined *before* the subscription is opened.
	def callback(message):
	   	
	   	source = "Stubhub"
	   	# datatype = "inventory"

	   	time_obj = datetime.utcnow()

	   	year = time_obj.year
	   	file = time_obj.strftime("%m_%d_%H-%M-%S")

	   	print ("Received message!")
	   	message.ack()

	   	try:
	   		team = str(eval(message.data)['team'])
	   	except:
	   		team = 'Unknown-Team'

	   	gcp_filename = "%s/%s/%s/%s/%s.txt" %(source, datatype, team, str(year), file)

	   	local_filename = str(int(time.time()))+ '.txt'

	   	write = False
	   	with open(local_filename,'w+') as f:
	   		try:
	   			f.write(str(eval(message.data)))
	   			write=True
	   		except:
	   			print("Not valid JSON, error writing to: " + local_filename)

	   	if write:
	   		upload_object(bucket, local_filename, gcp_filename, [], [])
	   	try:
	   		os.remove(local_filename)
	   	except:
	   		print("Nothing to delete")

	   

	# Open the subscription, passing the callback.
	future = subscription.open(callback)

	future.result()

if __name__=="__main__":
	
    parser = argparse.ArgumentParser(
    description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('sub_name', help='The name of the subscription')
    parser.add_argument('datatype', help='data type - folder in Storage to store')

    args = parser.parse_args()
    
    open_subscription(args.sub_name, args.datatype)