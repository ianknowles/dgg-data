"""Class for collecting reach estimates from the Facebook Marketing API. Run to collect a dataset for today."""
from collections import deque
import csv
import datetime
import json
import os
import time

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.targetingsearch import TargetingSearch
from facebook_business.api import FacebookAdsApi
from facebook_business.api import FacebookSession
from facebook_business.exceptions import FacebookRequestError

from storage.dgg_file_structure import auth_path
from storage.dgg_file_structure import data_path
from dgg_log import logging_setup
from collection.facebook_requests import create_country_target_queue
from storage.S3_bucket import S3Bucket
from storage import estimate_store
from dgg_log import root_logger

logger = root_logger.getChild(__name__)

facebook_app_auth = os.path.join(auth_path, 'app.json')
facebook_main_auth = os.path.join(auth_path, 'main_token.json')
facebook_backup_auth = os.path.join(auth_path, 'backup_token.json')


class FacebookCollection:
	"""Represents a collection session"""
	def __init__(self, batch_string, access_token=None):
		# if access_token:
			# self.access_token = access_token
		# else:
		with open(facebook_app_auth, 'r') as app_file, open(facebook_main_auth, 'r') as main_token_file, open(facebook_backup_auth, 'r') as backup_token_file:
			app_dict = json.load(app_file)
			main_dict = json.load(main_token_file)
			backup_dict = json.load(backup_token_file)

			main_session = FacebookSession(**app_dict, access_token=main_dict['token'])
			backup_session = FacebookSession(**app_dict, access_token=backup_dict['token'])

			self.main_api = FacebookAdsApi(main_session)
			self.backup_api = FacebookAdsApi(backup_session)

			self.main_account = AdAccount(main_dict['account'], api=self.main_api)
			self.backup_account = AdAccount(backup_dict['account'], api=self.backup_api)
		# if not self.access_token:
			# raise ValueError('No access token')

		self.sleep = 0
		self.usemain = True
		self.account = self.main_account
		self.main_last_error = time.time()
		self.queues = []
		self.countries = []
		self.batch_string = batch_string
		store_path = os.path.join(data_path, 'store_{timestamp}.json'.format(timestamp=batch_string))
		self.store = estimate_store.FacebookEstimateJsonStore(store_path)

	def handle_request_limit(self):
		"""Switch ad accounts to another available account or sleep if we have none left"""
		if self.usemain:
			self.usemain = False
			self.account = self.backup_account
			self.main_last_error = time.time()
			logger.info('Switching to backup account')
		elif time.time() - self.main_last_error > ((5*60) + 1):
			self.usemain = True
			self.account = self.main_account
			logger.info('Returning to main account')
		else:
			self.sleep = ((5*60) + 1)
			logger.warning('Both ad accounts over use limit')
			logger.info('sleeping {x} s'.format(x=self.sleep))
			time.sleep(self.sleep)

	def get_estimate(self, request):
		"""Send the given request to the server and validate the response. Store the response if valid."""
		try:
			# response = my_account.get_reach_estimate(params=params)
			request.attempts += 1
			request.response = self.account.get_delivery_estimate(params=request.params)
		except FacebookRequestError as e:
			# print(response)
			# https://developers.facebook.com/docs/marketing-api/error-reference/
			# https://developers.facebook.com/docs/graph-api/using-graph-api/error-handling/
			if e.api_error_code() == 1:
				logger.warning('(API Error 1) Unknown server error, continuing')
			elif e.api_error_code() == 2:
				logger.warning('(API Error 2) Marketing API service unavailable, retrying')
			elif e.api_error_code() == 4:
				# TODO application call limit, how long do we need to wait?
				sleeptime = 600
				logger.warning('(API Error 4) Application call limit reached, sleeping for {n} s'.format(n=sleeptime))
				time.sleep(sleeptime)
			elif e.api_error_code() == 10:
				logger.exception('(API Error 10) ??? Unhandled exception')
				raise e
			elif e.api_error_code() == 17:
				logger.warning('(API Error 17) Account call limit reached')
				self.handle_request_limit()
			elif e.api_error_code() == 100:
				logger.exception('(API Error 100) Invalid parameter, inputs need updating')
				# TODO error stats
				# self.store.record_error(request['code'])
				request.complete()
			elif e.api_error_code() == 102:
				logger.exception('(API Error 102) ??? Unhandled exception')
				raise e
			elif e.api_error_code() == 104:
				logger.exception('(API Error 104) ??? Unhandled exception')
				raise e
			elif e.api_error_code() == 190:
				if self.usemain:
					self.usemain = False
					logger.warning('(API Error 190) Main account credentials expired, switching to backup account')
					self.account = self.backup_account
				else:
					raise e
			elif e.api_error_code() == 200:
				logger.exception('(API Error 200) ??? Unhandled exception')
				raise e
			elif e.api_error_code() == 294:
				logger.exception('(API Error 294) ??? Unhandled exception')
				raise e
			else:
				logger.exception('Unknown Facebook API error code')
				raise e
		except TypeError as e:
			logger.warning('Internal Facebook Python API error, probable response format error. {e}'.format(e=e))
		except Exception:
			logger.exception('Unhandled Facebook Python API error')
		else:
			# print(response)
			if 'estimate_ready' in request.response[0]:
				if request.response[0]['estimate_ready']:
					request.valid = True
					if (request.response[0]['estimate_dau'] > 0) and (request.response[0]['estimate_mau'] > 0):
						# TODO check delta from previous collection is within reasonable range
						request.complete()
						behavior = None
						if 'behaviors' in request.params['targeting_spec']:
							behavior = request.params['targeting_spec']['behaviors'][0].get('name')
						gender = None
						if 'genders' in request.params['targeting_spec']:
							if request.params['targeting_spec']['genders'][0] == 1:
								gender = 'men'
							elif request.params['targeting_spec']['genders'][0] == 2:
								gender = 'women'
						else:
							gender = 'all'
						self.store.add_entry(request.params['targeting_spec']['geo_locations']['countries'][0], gender, request.params['targeting_spec'].get('age_min'), request.params['targeting_spec'].get('age_max'), behavior, request.response[0]['estimate_dau'], request.response[0]['estimate_mau'])
			elif request.retries > 100:
				alpha2 = request.params['targeting_spec']['geo_locations']['countries'][0]
				logger.error('Giving up on fetching response for {a2}'.format(a2=alpha2))
				logger.error(request)
				request.complete()
				# store.record_error(alpha3)

	def fetch_countries_list(self):
		"""
		Fetch the current list of countries.
		In practice this will rarely change but any change to the list will be handled without requiring code changes.
		"""
		# TODO should retry this a few times if it fails
		try:
			self.countries = TargetingSearch.search(params={
				'q': '',
				'type': TargetingSearch.TargetingSearchTypes.country,
				'limit': 1000,
			}, api=self.backup_api)
		except FacebookRequestError as e:
			if e.api_error_code() == 190:
				logger.error('(API Error 190) Backup account credentials expired, cannot continue')
				# TODO email error email
				raise SystemExit
			else:
				logger.exception('Unknown Marketing API error while fetching countries list, cannot continue')
				# TODO email error email
				raise SystemExit
		except Exception:
			# TODO try to enumerate some possible errors but we may want to retry in all circumstances
			logger.exception('Unknown Marketing API error while fetching countries list, cannot continue')

	def create_target_queue(self):
		"""Fetch the current list of countries and use it to prepare a queue of collection requests"""
		self.fetch_countries_list()
		print(self.countries)
		self.queues = []
		for count, country in enumerate(self.countries, start=1):
			self.queues += [{
				'code': country['country_code'],
				'queue': create_country_target_queue(country['country_code']),
				'count': count
			}]

	def collect(self):
		"""
		Run the main collection task.
		Initialises the request queues then repeatedly sends requests to the server until we have valid responses.
		Stops at 23:00 to allow for analysis and avoid overlap with tomorrow's collection
		"""
		logger.info('Beginning collection for {datestamp}'.format(datestamp=self.batch_string))
		repeats = []
		# TODO more collection stats, how many requests did we send, how many responses, how many errors?
		requesttotal = 0
		for queue in self.queues:
			requesttotal += len(queue['queue'])
		logger.info('{x} requests in master queue'.format(x=requesttotal))
		for queue in self.queues:
			logger.info('{n}/{t} starting queue for {c}'.format(n=queue['count'], t=len(self.queues), c=queue['code']))
			logger.info('{x} requests in queue'.format(x=len(queue['queue'])))
			complete = 0
			for item in queue['queue']:
				self.get_estimate(item)
				if not item.completed:
					repeats += [item]
				else:
					complete += 1
			logger.info('Finished first pass of {a2} queue, completed {x}/{n} requests'.format(a2=queue['code'], x=complete, n=len(queue['queue'])))

			# TODO consider restoring logfile upload during collection, probably uneccessary now
			# with open(log_filename, 'rb') as file:
				# key = '{folder}/{filename}'.format(folder=batch_s3_folder, filename=log_filename)
				# s3_bucket.put(key, file)

		logger.info('{x} requests to repeat'.format(x=len(repeats)))
		repeats = deque(repeats)
		while len(repeats) and (datetime.datetime.now().time() < datetime.time(hour=23, minute=0, second=0, microsecond=0)):
			item = repeats.pop()
			self.get_estimate(item)
			if not item.completed:
				repeats.appendleft(item)
			else:
				if not (len(repeats) % 100):
					logger.info('{x} requests remaining'.format(x=len(repeats)))

		self.store.write()
		s3_bucket = S3Bucket()
		self.store.upload(s3_bucket, self.batch_string)

		logger.info('Collection {batch} complete'.format(batch=self.batch_string))
		logger.info('{x}/{n} requests completed'.format(x=(requesttotal - len(repeats)), n=requesttotal))
		valid_zeroes = 0
		for request in repeats:
			if request.valid:
				valid_zeroes += 1
		errors = len(repeats) - valid_zeroes
		logger.info('{x}/{n} requests incomplete due to server returning zero sized populations'.format(x=valid_zeroes, n=requesttotal))
		logger.info('{x}/{n} requests incomplete due to errors'.format(x=errors, n=requesttotal))

	def collect_targeting_specs(self):
		"""Collect some lists of targeting specs to help choose new targeting parameters."""
		user_devices = TargetingSearch.search(params={
			'q': 'user_device',
			'type': TargetingSearch.TargetingSearchTypes.targeting_category,
			'limit': 1000,
		}, api=self.backup_api)
		print(user_devices)
		with open(os.path.join(data_path, 'devices.json'), 'w') as file, open(os.path.join(data_path, 'devices.csv'), 'w', newline='') as csvfile:
			d = list(filter(lambda x: x['type'] == 'user_device', map(lambda x: x._data, user_devices)))
			# d2 = list(filter(lambda x: x['type'] == 'user_device', d))
			d3 = {"root": d}
			json.dump(d3, file)
			csvwriter = csv.DictWriter(csvfile, fieldnames=d[0].keys())
			csvwriter.writeheader()
			csvwriter.writerows(d)

		user_os = TargetingSearch.search(params={
			'q': 'user_os',
			'type': TargetingSearch.TargetingSearchTypes.targeting_category,
			'limit': 1000,
		}, api=self.backup_api)
		print(user_os)
		with open(os.path.join(data_path, 'os.json'), 'w') as file, open(os.path.join(data_path, 'os.csv'), 'w', newline='') as csvfile:
			d = list(filter(lambda x: x['type'] == 'user_os', map(lambda x: x._data, user_os)))
			json.dump({"root": d}, file)
			csvwriter = csv.DictWriter(csvfile, fieldnames=d[0].keys())
			csvwriter.writeheader()
			csvwriter.writerows(d)


if __name__ == "__main__":
	logging_setup()
	batch = str(datetime.date.today().isoformat())
	session = FacebookCollection(batch)
	session.create_target_queue()
	session.collect()
