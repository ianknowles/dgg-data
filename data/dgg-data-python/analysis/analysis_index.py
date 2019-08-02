"""Classes representing the analysis index file. Run this script to download all data"""
import json
import os

from storage.S3_bucket import S3Bucket
from storage.dgg_file_structure import data_path
from storage.dgg_file_structure import project_path

INDEX_LOCATION = 'data/models.json'


class ModelIndexFile:
	"""Represents the analysis index file, synchronising a local copy to the bucket"""
	def __init__(self, bucket):
		self.bucket = bucket
		self.models = None

	def add_entry(self, entry_date, entry_path):
		"""Add a new analysis to the index"""
		self.fetch()
		self.add_local_entry(entry_date, entry_path)
		self.store()

	def update_latest(self, latest_path):
		"""Update the pointer to the latest analysis"""
		self.fetch()
		self.update_local_latest(latest_path)
		self.store()

	def add_latest(self, latest_date, latest_path):
		"""Add a new analysis and set the latest pointer to it"""
		self.fetch()
		self.add_local_entry(latest_date, latest_path)
		self.update_local_latest(latest_path)
		self.store()

	def add_local_entry(self, entry_date, entry_path):
		"""Add an analysis to the local copy of the index without synchronising to the bucket"""
		if self.models:
			self.models[entry_date] = entry_path

	def update_local_latest(self, latest_path):
		"""Update the local pointer to the latest analysis without synchronising to the bucket"""
		if self.models:
			self.models['latest'] = latest_path

	def fetch(self):
		"""Get the latest copy of the index from the bucket"""
		# TODO exception handling
		response = self.bucket.get(INDEX_LOCATION)
		self.models = json.loads(response['Body'].read())

	def store(self):
		"""Store the local index in the bucket"""
		# TODO exception handling
		# TODO just use dumps instead of dump?
		# would probably have to do binary encoding
		local_filepath = os.path.join(data_path, 'models.json')

		with open(local_filepath, 'w') as file:
			json.dump(self.models, file)

		with open(local_filepath, 'rb') as file:
			self.bucket.put(INDEX_LOCATION, file)

	def download_model(self, date, outputpath):
		if date in self.models:
			# store the filename in index
			filename = self.models[date].split('/')[-1]
			filepath = os.path.join(outputpath, filename)
			with open(filepath, 'wb') as outputfile:
				#TODO response checks
				response = self.bucket.get(self.models[date])
				while outputfile.write(response['Body'].read(amt=512)):
					pass

	def download_all_models(self, outputpath):
		for date in self.models:
			# store the filename in index
			filename = self.models[date].split('/')[-1]
			filepath = os.path.join(outputpath, filename)
			with open(filepath, 'wb') as outputfile:
				#TODO response checks
				response = self.bucket.get(self.models[date])
				while outputfile.write(response['Body'].read(amt=512)):
					pass


def add_todays_model_example():
	"""Example for adding latest analysis into the index, lacks checks and key format is old"""
	import datetime

	s3_bucket = S3Bucket()
	index = ModelIndexFile(s3_bucket)
	batch_string = str(datetime.date.today().isoformat())
	batch_s3_folder = 'data/{timestamp}'.format(timestamp=batch_string)
	key = '{folder}/monthly_model_{timestamp}.csv'.format(folder=batch_s3_folder, timestamp=batch_string)
	index.add_latest(batch_string, key)


if __name__ == "__main__":
	s3_bucket = S3Bucket()
	index = ModelIndexFile(s3_bucket)
	data_test_path = os.path.join(project_path, 'data_download')
	if not os.path.exists(data_test_path):
		os.makedirs(data_test_path)
	index.fetch()
	index.download_all_models(data_test_path)
