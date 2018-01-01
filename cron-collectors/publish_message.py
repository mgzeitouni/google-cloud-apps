from google.cloud import pubsub

def publish_message(topic_name, data, sport, team):

	try:
		project = 'kartees-188316'
		publisher = pubsub.PublisherClient()
		topic_path = publisher.topic_path(project, topic_name)

		data = str(data).encode('utf-8')
		publisher.publish(topic_path, data=data)

		return "Success, published message: %s - %s" %(sport, team)
	except:
		return "Error, message not published"

if __name__=='__main___':

	publish_messages('kartees-188316','raw_stubhub_event_inventory')