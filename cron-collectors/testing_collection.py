from collect_stubhub_cron import *


if __name__ == '__main__':

	logger = logging.getLogger('test')
	logger.setLevel(logging.DEBUG)

	# create a file handler writing to a file named after the thread
	file_handler = logging.FileHandler('test.log')

	# create a custom formatter and register it for the file handler
	formatter = logging.Formatter('(%(threadName)-10s) %(message)s')
	file_handler.setFormatter(formatter)

	# register the file handler for the thread-specific logger
	logger.addHandler(file_handler)

	dataType = "inventory"
	path = "event_inventory"
	account="LABO"
	call_API_for_data(dataType, path, account, logger)