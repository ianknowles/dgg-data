import csv
import datetime
import functools
import json
import os
import string
import time
from collections import deque
from zipfile import ZipFile, ZIP_LZMA

import pycountry
import pykka
from facebook_business.adobjects.targetingsearch import TargetingSearch
from facebook_business.api import FacebookSession, FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

from facebook_requests import create_request_queue, populations
from dgg_log import logging_setup
from dgg_log import root_logger
from storage.S3_bucket import S3Bucket
from storage.dgg_file_structure import data_path, auth_path

logger = root_logger.getChild(__name__)
# TODO continue collection from file
# TODO export results
# TODO session factory to create new sessions if one crashes
# TODO supervisor to allocate usernames and tasks
# TODO check progress on exit?
# TODO deal with reequests queued that may not execute until after the collection times out?
# TODO switch to callback processing of results not post-queue processing

facebook_app_auth = os.path.join(auth_path, 'app.json')
facebook_main_token = os.path.join(auth_path, 'main_token.json')

class EmailActor(pykka.ThreadingActor):
	def on_receive(self, message):
		logger.info(f"{self} received {message['messsagetype']}")


class S3BucketActor(pykka.ThreadingActor):
	def __init__(self, folder, zipfilename, s3_bucket=S3Bucket()):
		super().__init__()
		self.s3_bucket = s3_bucket
		self.s3_folder = f"data/{folder}"
		self.zipfilename = f'{zipfilename}.zip'
		self.zipfilepath = os.path.join(data_path, self.zipfilename)
		self.files = []

	def add(self, filepath):
		self.files.append(filepath)

	def add_files(self, filelist):
		self.files += filelist

	def upload(self):
		logger.info(f"Uploading files to {self.s3_folder}")

		for filepath in self.files:
			key = f"{self.s3_folder}/{os.path.basename(filepath)}"
			with open(filepath, 'rb') as file:
				self.s3_bucket.put(key, file)

	def upload_as_zip(self):
		logger.info(f'Preparing bucket zipfile at {self.zipfilepath}')
		logger.info(f'Uploaded logfile will end here after insertion to archive')
		logger.info(f'Local logfile will detail any upload problems past this point')
		try:
			with ZipFile(self.zipfilepath, 'w', compression=ZIP_LZMA) as zipfile:
				for filepath in self.files:
					zipfile.write(filepath, arcname=os.path.basename(filepath))
		except:
			logger.exception(f'Error saving collection zipfile to {self.zipfilepath}')
			raise
		else:
			logger.info(f'Local logfile continues...')
			logger.info(f'Bucket zipfile saved to {self.zipfilepath}')

			key = f"{self.s3_folder}/{self.zipfilename}"
			logger.info(f"Uploading zipfile to {key}")
			with open(self.zipfilepath, 'rb') as file:
				self.s3_bucket.put(key, file)


class FacebookAdsSessionActor(pykka.ThreadingActor):
	def __init__(self, app_id, app_secret, access_token, ad_account_id):
		super().__init__()
		self.app_id = app_id
		self.app_secret = app_secret
		self.access_token = access_token
		self.session = None
		self.ads_api = None
		self.ad_account = None
		self.ad_account_id = ad_account_id
		self.subscribers = []
		self.start_time = None
		self.targeting_search_stats = {'success_count': 0, 'error_count': 0}
		self.delivery_estimate_stats = {'success_count': 0, 'error_count': 0}
		self.next_time = None

	def subscribe(self, actorref):
		self.subscribers.append(actorref)

	def on_start(self):
		# TODO any validation of auth
		# member variables dict?
		#TODO connect test and error handling
		self.start_time = datetime.datetime.now(datetime.timezone.utc)
		self.next_time = self.start_time
		self.session = FacebookSession(self.app_id, self.app_secret, self.access_token)
		self.ads_api = FacebookAdsApi(self.session)
		self.ad_account = AdAccount(self.ad_account_id, api=self.ads_api)

	def on_stop(self):
		logger.info('Session stopped')

	def on_failure(self, exception_type, exception_value, traceback):
		# My optional cleanup code in same context as on_receive()
		#pykka.ActorRegistry.broadcast(message, target_class=EmailActor)
		logger.warning('Session stopping due to unhandled error')
		self.tell_subscribers({'messsagetype': 'UNHANDLED', 'exception_type': exception_type, 'exception_value': exception_value, 'traceback': traceback})

	#def on_receive(self, message):
		... # My optional message handling code for a plain actor

	def tell_subscribers(self, msg):
		for subscriber in self.subscribers:
			subscriber.tell(msg)

	def targeting_search(self, search_params):
		"""
		Fetch the current list of countries.
		In practice this will rarely change but any change to the list will be handled without requiring code changes.
		"""
		# TODO should retry this a few times if it fails
		result = []
		try:
			result = TargetingSearch.search(search_params, api=self.ads_api)
		except FacebookRequestError as e:
			self.targeting_search_stats['error_count'] += 1
			self.handle_facebook_request_error(e)
		else:
			self.targeting_search_stats['success_count'] += 1
		return result

	def handle_facebook_request_error(self, e):

		# ######TODO####### basically all of these should reraise now as the collector is expecting to receive a good
		# result


		print(e)
		# https://developers.facebook.com/docs/marketing-api/error-reference/
		# https://developers.facebook.com/docs/graph-api/using-graph-api/error-handling/
		if e.http_status() != 400:
			logger.exception('HTTP Status Error')
			#TODO Handling for 503 service unavailable
		elif e.api_error_code() == 1:
			logger.warning('(API Error 1) Unknown server error, continuing')
		elif e.api_error_code() == 2:
			logger.warning('(API Error 2) Marketing API service unavailable, retrying')
			self.next_time = self.next_time + datetime.timedelta(seconds=60)
		elif e.api_error_code() == 4:
			# TODO application call limit, how long do we need to wait?
			sleep_time = 600
			logger.warning(f'(API Error 4) Application call limit reached, sleeping for {sleep_time} s')
			self.next_time = self.next_time + datetime.timedelta(seconds=sleep_time)
		elif e.api_error_code() == 10:
			logger.exception('(API Error 10) ??? Unhandled exception')
		elif e.api_error_code() == 17:
			logger.warning('(API Error 17) Account call limit reached')
			self.next_time = self.next_time + datetime.timedelta(seconds=300)
		elif e.api_error_code() == 100:
			logger.exception('(API Error 100) Invalid parameter, inputs need updating')
			# TODO error stats
			# self.store.record_error(request['code'])
		elif e.api_error_code() == 102:
			logger.exception('(API Error 102) ??? Unhandled exception')
		elif e.api_error_code() == 104:
			logger.exception('(API Error 104) ??? Unhandled exception')
		elif e.api_error_code() == 190:
			logger.error('(API Error 190) Account credentials expired')
			# TODO report and mark account credentials as bad
		elif e.api_error_code() == 200:
			logger.exception('(API Error 200) ??? Unhandled exception')
		elif e.api_error_code() == 294:
			logger.exception('(API Error 294) ??? Unhandled exception')
		elif e.api_error_code() == 80004:
			logger.warning(e.api_error_message())
			self.next_time = self.next_time + datetime.timedelta(seconds=300)
			logger.warning('sleeping for 300 s')
		else:
			logger.exception('Unknown Facebook API error code')
		raise

	def get_estimate(self, params):
		"""Send the given request to the server and validate the response. Store the response if valid."""
		response = None
		timestamp = None
		try:
			sleep_time = (self.next_time - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
			if sleep_time > 0:
				time.sleep(sleep_time)
			self.next_time = self.next_time + datetime.timedelta(seconds=12)
			# response = my_account.get_reach_estimate(params=params)
			response = self.ad_account.get_delivery_estimate(params=params)
			timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp()
			print(response)
		except FacebookRequestError as e:
			self.delivery_estimate_stats['error_count'] += 1
			self.handle_facebook_request_error(e)
		except TypeError as e:
			self.delivery_estimate_stats['error_count'] += 1
			logger.exception('Internal Facebook Python API error, probable response format error.')
			raise
		except Exception:
			self.delivery_estimate_stats['error_count'] += 1
			logger.exception('Unhandled Facebook Python API error')
			raise
		else:
			self.delivery_estimate_stats['success_count'] += 1
		return response, timestamp


StartCollect = object()


class FacebookAdsCollectionActor(pykka.ThreadingActor):
	def __init__(self, session, requests, name):
		super().__init__()
		self.session = session
		self.queue = deque(requests)
		self.completed = deque()
		self.request_total = len(requests)
		self.start = None
		self.name = name
		self.filename = ''
		self.sent_requests = 0

	def on_start(self):
		# TODO verify queue correctness?
		logger.info(f'Initialised collection {self.name} with {self.request_total} requests in queue')

	def on_stop(self):
		logger.info(f'Stopping collection actor {self.name} for {self.start.date().isoformat()}')
		self.save()
		self.save_csv()

	def on_failure(self, exception_type, exception_value, traceback):
		# My optional cleanup code in same context as on_receive()
		#pykka.ActorRegistry.broadcast(message, target_class=EmailActor)
		logger.warning('Collection stopping due to unhandled error')
		self.on_stop()
		#self.tell_subscribers({'messsagetype': 'UNHANDLED', 'exception_type': exception_type, 'exception_value': exception_value, 'traceback': traceback})

	def collect(self):
		"""
		Run the main collection task.
		Initialises the request queues then repeatedly sends requests to the server until we have valid responses.
		Stops at 23:00 to allow for analysis and avoid overlap with tomorrow's collection
		"""
		self.start = datetime.datetime.now(datetime.timezone.utc)
		self.filename = f'{self.start.date().isoformat()}_{self.name}_collection'
		logger.info(f'Beginning collection {self.name} for {self.start.date().isoformat()}')
		repeats = []
		# TODO more collection stats, how many requests did we send, how many responses, how many errors?

		#futures = map(lambda r: self.session.get_estimate(r.params).map(verify_result), self.queue)
		for request in self.queue:
			request.future = self.session.get_estimate(request.params)
		#self.queue = deque()

		while len(self.queue) and (datetime.datetime.now(datetime.timezone.utc).time() < datetime.time(hour=23, minute=59, second=0, microsecond=0)):
			request = self.queue.popleft()
			timestamp = None
			try:
				request.attempts += 1
				self.sent_requests += 1
				response, timestamp = request.future.get(timeout=660)
				#print(response)
			except pykka.Timeout:
				logger.warning(f'Request timeout. {request.attempts} attempts')
			except FacebookRequestError as e:
				if e.api_error_code() == 100:
					request.complete()
					#self.tell_subscribers({'messsagetype': 'INPUTERROR', 'params': request.params})
			except Exception as e:
				#print(e)
				pass
			else:
				#print('Response check')
				if 'estimate_ready' in response[0]:
					if response[0]['estimate_ready']:
						request.valid = True
						matches = 0
						#for r in request.responses:
							#if (r['estimate_dau'] == response[0]['estimate_dau']) and (r['estimate_mau'] == response[0]['estimate_mau']):
							#	matches += 1
							# if (response[0]['estimate_dau'] > 0) and (response[0]['estimate_mau'] > 0):
							# TODO check delta from previous collection is within reasonable range
						#if matches > 1:
						if response[0]['estimate_mau_lower_bound'] >= 1000:
							request.complete()
				request.responses.append({'t': timestamp, **response[0].export_all_data()})
			finally:
				...
				# check session is still alive, passs in session factory rather than session? ask factory for new session

			if not request.completed and request.attempts > 100:
				logger.warning(f'Giving up on fetching response after {request.attempts} attempts')
				logger.debug(request.params)
				logger.debug(request.responses[-1])
				request.complete()

			if not request.completed:
				self.queue.append(request)
			else:
				self.completed.append(request)
				remain = len(self.queue)
				if not (remain % 100):
					logger.info(f'{remain} requests remaining')
				if not (remain % 1000):
					self.save()
			if not (self.sent_requests % 1000):
				logger.info(f'{self.sent_requests} requests sent')
				logger.info(f'{len(self.completed)}/{self.request_total} requests completed')

		logger.info(f'Finished collection {self.name} for {self.start.date().isoformat()}')
		logger.info(f'{len(self.completed)}/{self.request_total} requests completed')
		if len(self.queue):
			valid_zeroes = functools.reduce(lambda x, r: x + 1 if r.valid else x, self.queue)
			errors = len(self.queue) - valid_zeroes
		else:
			valid_zeroes = 0
			errors = 0
		logger.info(f'{valid_zeroes}/{self.request_total} requests incomplete due to server returning inconsistent data')
		logger.info(f'{errors}/{self.request_total} requests incomplete due to errors')

		return [self.save(), self.save_csv()]

	def save(self):
		json_filename = f'{self.filename}.json'
		json_filepath = os.path.join(data_path, json_filename)
		jsonrepr = {'start': self.start.timestamp(), 'queue': [r.to_dict() for r in self.queue], 'complete': [r.to_dict() for r in self.completed]}
		try:
			with open(json_filepath, 'w', encoding='utf8') as file:
				json.dump(jsonrepr, file)
		except:
			logger.exception(f'Error saving collection to {json_filepath}')
			raise
		else:
			logger.info(f'Collection saved to {json_filepath}')
			return json_filepath

	def save_csv(self):
		csv_filename = f'{self.filename}.csv'
		csv_filepath = os.path.join(data_path, csv_filename)
		fieldnames = {}
		rows = [r.to_flat_dict() for r in self.completed]
		for r in rows:
			#print(r)
			if r is not None:
				#print(r.keys())
				fieldnames.update(r)
		try:
			with open(csv_filepath, 'w', encoding='utf8', newline='') as file:
				writer = csv.DictWriter(file, fieldnames=list(fieldnames))

				writer.writeheader()
				writer.writerows(rows)
		except:
			logger.exception(f'Error saving collection to {csv_filepath}')
			raise
		else:
			logger.info(f'Collection saved to {csv_filepath}')
			return csv_filepath

	def save_large(self):
		filename = f'{self.filename}.json'
		filepath = os.path.join(data_path, filename)
		queue_filename = f'{self.start.date().isoformat()}_{self.name}_queue.json'
		queue_filepath = os.path.join(data_path, queue_filename)
		completed_filename = f'{self.start.date().isoformat()}_{self.name}_completed.json'
		completed_filepath = os.path.join(data_path, completed_filename)
		jsonrepr = {'start': self.start.timestamp(), 'queue': queue_filename, 'complete': completed_filename}
		try:
			with open(filepath, 'w', encoding='utf8') as file:
				json.dump(jsonrepr, file)
		except:
			logger.exception(f'Error saving collection to {filepath}')
			raise
		else:
			logger.info(f'Collection saved to {filepath}')

		try:
			with open(queue_filepath, 'w', encoding='utf8') as file:
				for r in self.queue:
					json.dump(r.to_dict(), file)
					file.write('\n')
		except:
			logger.exception(f'Error saving queue to {queue_filepath}')
			raise
		else:
			logger.info(f'Queue saved to {queue_filepath}')

		try:
			with open(completed_filepath, 'w', encoding='utf8') as file:
				for r in self.completed:
					json.dump(r.to_dict(), file)
					file.write('\n')
		except:
			logger.exception(f'Error saving completed requests to {completed_filepath}')
			raise
		else:
			logger.info(f'Completed requests saved to {completed_filepath}')

		zip_filepath = os.path.join(data_path, f'{self.filename}.zip')
		try:
			with ZipFile(zip_filepath, 'w', compression=ZIP_LZMA) as zipfile:
				zipfile.write(filepath, arcname=filename)
				zipfile.write(queue_filepath, arcname=queue_filename)
				zipfile.write(completed_filepath, arcname=completed_filename)
		except:
			logger.exception(f'Error saving collection zipfile to {zip_filepath}')
			raise
		else:
			logger.info(f'Collection zipfile saved to {zip_filepath}')


def verify_result(result):
	...


def cities(code):
	email = EmailActor.start().proxy()
	app_auth = {}
	token_auth = {}
	with open(facebook_app_auth, 'r') as app_file, open(facebook_main_token, 'r') as main_token_file:
		app_auth = json.load(app_file)
		token_auth = json.load(main_token_file)
	adssession = FacebookAdsSessionActor.start(**app_auth, **token_auth).proxy()
	adssession.subscribe(email)
	cities = {}
	for letter in string.ascii_lowercase:
		locations = adssession.targeting_search({
			'q': letter,
			'type': TargetingSearch.TargetingSearchTypes.geolocation,
			'location_types': ['city'],
			'country_code': code,
			'limit': 750,
		}).get()
		if len(locations) >= 750:
			logger.warning(f'Too many responses for {letter}')
		for location in locations:
			if location['type'] == 'city':
				cities[location['key']] = location.export_all_data()
	logger.info(f'Found {len(cities)} cities in {code}')
	cities_list_filepath = os.path.join(data_path, f'{datetime.datetime.now(datetime.timezone.utc).date().isoformat()}_{code}_cities.json')
	files = [cities_list_filepath]
	with open(cities_list_filepath, 'w', encoding='utf8') as file:
		json.dump(cities, file, indent='\t')

	collect = FacebookAdsCollectionActor.start(adssession, populations(cities.values()), f'{code}_cities_pop').proxy()
	try:
		files += collect.collect().get()
	except:
		logger.exception("Unhandled collection exception occurred.")
	cities_by_size = sorted(filter(lambda r: r.responses, collect.completed.get()), key=lambda r: r.responses[-1]['estimate_mau_upper_bound'], reverse=True)
	most_populous_cities = cities_by_size[:10]
	cities_to_collect = []
	for city in most_populous_cities:
		city_key = city.params["targeting_spec"]["geo_locations"]["cities"][0]["key"]
		cities_to_collect.append(cities[city_key])
	collect2 = FacebookAdsCollectionActor.start(adssession, create_request_queue(cities_to_collect), f'{code}_largest_100_cities').proxy()
	try:
		files += collect2.collect().get()
	except:
		logger.exception("Unhandled collection exception occurred.")
	collect.stop()
	collect2.stop()
	return files


def regions(code):
	email = EmailActor.start().proxy()
	app_auth = {}
	token_auth = {}
	with open(facebook_app_auth, 'r') as app_file, open(facebook_main_token, 'r') as main_token_file:
		app_auth = json.load(app_file)
		token_auth = json.load(main_token_file)
	adssession = FacebookAdsSessionActor.start(**app_auth, **token_auth).proxy()
	adssession.subscribe(email)
	regions = {}
	for letter in string.ascii_lowercase:
		locations = adssession.targeting_search({
			'q': letter,
			'type': TargetingSearch.TargetingSearchTypes.geolocation,
			'location_types': ['region'],
			'country_code': code,
			'limit': 500,
		}).get()
		for location in locations:
			if location['type'] == 'region':
				regions[location['key']] = location.export_all_data()

	logger.info(f'Found {len(regions)} regions in {code}')
	regions_list_filepath = os.path.join(data_path, f'{datetime.datetime.now(datetime.timezone.utc).date().isoformat()}_{code}_regions.json')
	files = [regions_list_filepath]
	with open(regions_list_filepath, 'w', encoding='utf8') as file:
		json.dump(regions, file, indent='\t')
	collection = FacebookAdsCollectionActor.start(adssession, create_request_queue(regions.values()), f'{code}_regions').proxy()
	try:
		files += collection.collect().get()
	except:
		logger.exception("Unhandled collection exception occurred.")
	collection.stop()
	return files


if __name__ == '__main__':
	logfile = logging_setup()
	try:
		files = regions("AF")
		files += cities("AF")

		date = datetime.datetime.now(datetime.timezone.utc).date()
		bucket = S3BucketActor.start(f"AF/{date.year}/{date.month}/{date.day}", f"AF_collection_{date.isoformat()}").proxy()
		bucket.add_files(files + [logfile]).get()
		bucket.upload_as_zip().get()
		bucket.stop()

		#pycountry
		countries = ["NGA","ETH","COD","EGY","ZAF","TZA","KEN","UGA","DZA","SDN","MAR","MOZ","GHA","AGO","SOM","CIV","MDG","CMR","BFA","NER","MWI","ZMB","MLI","SEN","ZWE","TCD","TUN","GIN","RWA","BEN","BDI","SSD","ERI","SLE","TGO","LBY","CAF","MRT","COG","LBR","NAM","BWA","LSO","GMB","GAB","GNB","MUS","GNQ","SWZ","DJI","REU","COM","ESH","CPV","MYT","STP","SYC","SHN"]

		for country in countries:
			countryrecord = pycountry.countries.get(alpha_3=country)
			#files = regions(countryrecord.alpha_2)

			#date = datetime.datetime.now(datetime.timezone.utc).date()
			#bucket = S3BucketActor.start(f"{countryrecord.alpha_2}/{date.year}/{date.month}/{date.day}", f"{countryrecord.alpha_2}_collection_{date.isoformat()}").proxy()
			#bucket.add_files(files + [logfile]).get()
			#bucket.upload_as_zip().get()
			#bucket.stop()

		pykka.ActorRegistry.stop_all()
	except:
		logger.exception("Unhandled collection exception occurred.")
	pykka.ActorRegistry.stop_all()