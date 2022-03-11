import json
import os

from r_analysis_wrapper import predict
from dgg_log import root_logger
from storage.S3_bucket import S3Bucket
from storage.dgg_file_structure import auth_path

logger = root_logger.getChild(__name__)

s3_auth = os.path.join(auth_path, 'S3_keys.json')


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

	from analysis.analysis_index import ModelIndexFile
	s3_bucket = S3Bucket()
	index = ModelIndexFile(s3_bucket, 'data/models2.json')

	paginator = client.get_paginator('list_objects')
	result = paginator.paginate(Bucket='www.digitalgendergaps.org', Prefix='data/', Delimiter='/')
	for prefix in result.search('CommonPrefixes'):
		batch_string = prefix['Prefix'].split('/')[1]
		try:
			mau_key = predict(batch_string)

			index.add_entry(batch_string, mau_key)
			predict(batch_string, 'dau')
		except Exception as e:
			logger.error('Exception in batch {x}, {e}'.format(x=batch_string, e=e))


def redo_dates(dates):
	from analysis.analysis_index import ModelIndexFile

	s3_bucket = S3Bucket()
	index = ModelIndexFile(s3_bucket, 'data/models2.json')
	for date in dates:
		try:
			mau_key = predict(date)
			index.add_entry(date, mau_key)
			predict(date, 'dau')
		except Exception as e:
			logger.exception(f'Exception in batch {date}')
