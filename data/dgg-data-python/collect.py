"""Main script for the dgg-data-python package. Run to perform a collection and analysis for today."""
import datetime

from dgg_log import logging_setup
from dgg_log import root_logger

from dgg_email import send_log
from dgg_email import send_error_log

from analysis import r_analysis_wrapper
from collection.facebook_collector import FacebookCollection
from storage.S3_bucket import S3Bucket

logger = root_logger.getChild(__name__)


if __name__ == "__main__":
	date_stamp = str(datetime.date.today().isoformat())
	batch_s3_folder = 'data/{date_stamp}'.format(date_stamp=date_stamp)
	log_filepath = ''
	try:
		log_filepath = logging_setup()

		session = FacebookCollection(date_stamp)
		session.create_target_queue()
		session.collect()

		r_analysis_wrapper.predict(date_stamp)

		send_log(log_filepath, date_stamp)
	except Exception as e:
		logger.exception('Uncaught exception')
		send_error_log(log_filepath, date_stamp)
		raise e
	finally:
		with open(log_filepath, 'rb') as file:
			key = '{folder}/{filename}'.format(folder=batch_s3_folder, filename=log_filepath)
			s3_bucket = S3Bucket()
			s3_bucket.put(key, file)
