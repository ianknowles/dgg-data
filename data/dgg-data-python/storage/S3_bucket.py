"""Convenience classes representing an S3 bucket and wrapping some boto3 functions"""
import json
import os
import urllib.parse

import boto3

from storage.dgg_file_structure import auth_path
from dgg_log import root_logger

logger = root_logger.getChild(__name__)

s3_auth = os.path.join(auth_path, 'S3_keys.json')


class S3Bucket:
	"""Represents an S3 bucket, defaults to our usual bucket and loads the auth keys from a file in the auth folder"""
	def __init__(self, bucket='www.digitalgendergaps.org', key_filepath=s3_auth):
		self.bucket = bucket
		try:
			with open(key_filepath) as key_file:
				s3_keys = json.load(key_file)
				self.client = boto3.client('s3', **s3_keys)
		except EnvironmentError:
			logger.error('S3 credentials missing')

	def put(self, filepath, file_body):
		"""Put a binary file stream into the bucket with the given remote filepath"""
		# TODO exception handling
		logger.info("Saving file '{file}' to {bucket}".format(file=filepath, bucket=self.bucket))
		self.client.put_object(Bucket=self.bucket, Key=filepath, Body=file_body)

	def get(self, filepath):
		"""Get an object from the given remote filepath"""
		logger.info("Getting file '{file}' from {bucket}".format(file=filepath, bucket=self.bucket))
		return self.client.get_object(Bucket=self.bucket, Key=filepath)

	def get_folder(self, key):
		"""Create a folder object representing a remote filepath in the bucket"""
		return S3Folder(self, key)


# TODO use folder
class S3Folder:
	"""Represents a filepath in the bucket"""
	def __init__(self, bucket: S3Bucket, path):
		self.bucket: S3Bucket = bucket
		# TODO check that trailing / is actually needed
		self.path = path + '/'

	def put(self, filename, file_body):
		"""Puts a binary file stream into the bucket folder"""
		self.bucket.put(urllib.parse.urljoin(self.path, filename), file_body)
