"""Methods for running the R analysis script. Run this script to analyse today's collection"""
import datetime
import json
import os

from analysis import r_language
from storage.S3_bucket import S3Bucket
from analysis.preprocessing import preprocess_analysis_data
from dgg_log import logging_setup
from dgg_log import root_logger

from storage.dgg_file_structure import data_path
from storage.dgg_file_structure import r_path
from storage.dgg_file_structure import auth_path

logger = root_logger.getChild(__name__)

s3_auth = os.path.join(auth_path, 'S3_keys.json')


def cleanup_r_script_outputs(output_path):
	"""Delete any previous outputs from the R script if present"""
	try:
		os.remove(os.path.join(output_path, 'Appendix_table_model_predictions.csv'))
	except OSError:
		pass

	try:
		os.remove(os.path.join(output_path, 'GroundTruth_correlations_table.csv'))
	except OSError:
		pass

	try:
		os.remove(os.path.join(output_path, 'Rplots.pdf'))
		os.remove(os.path.join(output_path, 'Rplots1.pdf'))
		os.remove(os.path.join(output_path, 'Rplots2.pdf'))
		os.remove(os.path.join(output_path, 'Rplots3.pdf'))
		os.remove(os.path.join(output_path, 'Rplots4.pdf'))
	except OSError:
		pass

	try:
		os.remove(os.path.join(output_path, 'fits.csv'))
	except OSError:
		pass


def predict(batch_string, estimate='mau'):
	"""Run an analysis for the given day. Inputs are expected to be retrievable from S3"""
	preprocess_analysis_data(batch_string, estimate)

	s3_bucket = S3Bucket()

	cleanup_r_script_outputs(data_path)

	logger.info('Beginning analysis')
	r_exe = r_language.RExecutable()
	r_exe.run_script(os.path.join(r_path, "Digital_gender_gaps_analysis.R"), '')
	logger.info('Analysis complete')

	with open(os.path.join(data_path, 'Appendix_table_model_predictions.csv'), 'rb') as countfile:
		batch_s3_folder = 'data/{timestamp}'.format(timestamp=batch_string)
		key = '{folder}/{estimate}_monthly_model_{timestamp}.csv'.format(estimate=estimate, folder=batch_s3_folder, timestamp=batch_string)
		s3_bucket.put(key, countfile)

	return key


def catchup_analysis():
	"""Run an analysis for any collections that haven't been analysed yet"""
	import boto3

	with open(s3_auth) as key_file:
		s3_keys = json.load(key_file)
		client = boto3.client('s3', **s3_keys)

	paginator = client.get_paginator('list_objects')
	result = paginator.paginate(Bucket='www.digitalgendergaps.org', Prefix='data/', Delimiter='/')
	for prefix in result.search('CommonPrefixes'):
		batch_string = prefix['Prefix'].split('/')[1]
		try:
			response = client.get_object(Bucket='www.digitalgendergaps.org', Key=prefix['Prefix'] + 'monthly_model.csv')
		except client.exceptions.NoSuchKey:
			try:
				# TODO exceptions not for control flow, may be limited by boto3 available methods
				predict(batch_string)
			except Exception as e:
				logger.error('Exception in batch {x}, {e}'.format(x=batch_string, e=e))


def redo_analysis():
	"""Run a new analysis for all datasets in the bucket"""
	import boto3

	with open(s3_auth) as key_file:
		s3_keys = json.load(key_file)
		client = boto3.client('s3', **s3_keys)

	paginator = client.get_paginator('list_objects')
	result = paginator.paginate(Bucket='www.digitalgendergaps.org', Prefix='data/', Delimiter='/')
	for prefix in result.search('CommonPrefixes'):
		batch_string = prefix['Prefix'].split('/')[1]
		try:
			predict(batch_string)
		except Exception as e:
			logger.error('Exception in batch {x}, {e}'.format(x=batch_string, e=e))


if __name__ == "__main__":
	from storage.dgg_file_structure import log_path
	logging_setup(log_path)


	today_string = str(datetime.date.today().isoformat())
	predict(today_string)
	predict(today_string, 'dau')
