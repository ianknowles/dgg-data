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
			logger.error(f'S3 credentials failed to load from {key_filepath}')

	def put(self, file_key, file_body):
		"""Put a binary file stream into the bucket with the given remote filepath"""
		# TODO exception handling
		logger.info(f"Saving file '{file_key}' to {self.bucket}")
		self.client.put_object(Bucket=self.bucket, Key=file_key, Body=file_body)

	def get(self, file_key):
		"""Get an object from the given remote filepath"""
		logger.info(f"Getting file '{file_key}' from {self.bucket}")
		return self.client.get_object(Bucket=self.bucket, Key=file_key)

	def get_folder(self, path):
		"""Create a folder object representing a remote filepath in the bucket"""
		return S3Folder(self, path)


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

	def get(self, file_key):
		"""Get an object from the given remote filepath"""
		logger.info(f"Getting file '{file_key}' from {self.bucket}")
		return self.bucket.get(urllib.parse.urljoin(self.path, file_key))

	def get_folder(self, path):
		"""Create a folder object representing a remote filepath in the bucket"""
		return S3Folder(self.bucket, urllib.parse.urljoin(self.path, path))
