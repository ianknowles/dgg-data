"""Log setup and other functions"""
import datetime
import json
import logging
import logging.config
import os

from storage.dgg_file_structure import config_path

logging_config_filepath = os.path.join(config_path, 'log_config.json')
root_logger = logging.getLogger('dgg')
logger = root_logger.getChild(__name__)


# TODO log upload
# TODO log cleanup?
def logging_setup(log_path):
	"""Setup the loggers using a config file, return the filepath to .log file"""
	log_filepath = ''
	if not root_logger.hasHandlers():
		try:
			with open(logging_config_filepath, 'r') as file:
				config_dict = json.load(file)

				time_stamp_string = datetime.datetime.now().isoformat().replace(':', '.')
				log_filepath = os.path.join(log_path, f'{time_stamp_string}.log')

				config_dict['handlers']['file']['filename'] = log_filepath
				logging.config.dictConfig(config_dict)
		except EnvironmentError:
			logger.error(f'Logging config is missing, please provide {logging_config_filepath} before continuing')
			raise
	logger.info(f'Log file configured with {logging_config_filepath}')
	logger.info(f'Logging to {log_filepath}')
	return log_filepath
