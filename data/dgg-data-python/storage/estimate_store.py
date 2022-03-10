"""
Classes for data storage, different implementations for different underlying storage, JSON files, databases etc
via a common interface
"""
import time
import json
import os
import pathlib

from dgg_log import root_logger

logger = root_logger.getChild(__name__)


class FacebookEstimateJsonStore:
	"""Represents a store using a JSON file"""
	def __init__(self, filepath):
		self.filepath = filepath
		self.dictionary = {}
		# TODO use pathlib more generally
		if pathlib.Path(self.filepath).is_file():
			try:
				self.read()
			except json.decoder.JSONDecodeError:
				logger.info('Decoding error while loading estimate store from file "{filepath}" store object created empty'.format(**vars(self)))
				self.dictionary = {}

	def add_entry(self, country, gender, age_min, age_max, behaviour, dau, mau):
		"""Record a reach estimate into the store"""
		# TODO change to take a request object
		record = {'timestamp': time.time()} #, 'estimate_dau': dau, 'estimate_mau': mau}
		# estimate['age_min'] = age_min
		# estimate['age_max'] = age_max

		if age_max and age_max > age_min:
			age_key = '{min}-{max}'.format(min=age_min, max=age_max)
		else:
			age_key = '{min}+'.format(min=age_min)
		if country not in self.dictionary:
			self.dictionary[country] = {'errors': 0}
		if gender not in self.dictionary[country]:
			self.dictionary[country][gender] = {}
		if age_key not in self.dictionary[country][gender]:
			self.dictionary[country][gender][age_key] = {}
		if behaviour:
			record['estimate_dau'] = dau
			record['estimate_mau'] = mau
			self.dictionary[country][gender][age_key][behaviour] = record
		else:
			record['age_min'] = age_min
			if age_max:
				record['age_max'] = age_max
			record['estimate_dau'] = dau
			record['estimate_mau'] = mau
			self.dictionary[country][gender][age_key] = record

	def read(self):
		"""Load a store from a local JSON file"""
		with open(self.filepath, 'r') as file:
			logger.info('Loading estimate store from file {filepath}'.format(**vars(self)))
			self.dictionary = json.load(file)

	def write(self):
		"""Write the store to the local filesystem as a JSON file"""
		with open(self.filepath, 'w') as file:
			logger.info('Saving estimate store to file {filepath}'.format(**vars(self)))
			json.dump(self.dictionary, file)

	def upload(self, bucket, batch_string):
		"""Upload the store to our S3 bucket"""
		# TODO not a responsibility of this class rewrite
		# TODO could upload without a write
		# TODO take S3 folder, pass to folder
		self.write()

		batch_s3_folder = 'data/{timestamp}'.format(timestamp=batch_string)
		key = '{folder}/{filename}'.format(folder=batch_s3_folder, filename=os.path.basename(self.filepath))
		with open(self.filepath, 'rb') as file:
			bucket.put(key, file)

	def write_csv_file(self):
		# TODO implementation
		# TODO write a csv implementation of the store class and call its constructor here
		raise NotImplementedError
