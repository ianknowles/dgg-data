"""Methods for preprocessing the facebook count data into a format usable by the R analysis"""
import csv
import json
import os

from storage.S3_bucket import S3Bucket
from storage.dgg_file_structure import data_path
from dgg_log import root_logger

logger = root_logger.getChild(__name__)


def preprocess_counts_from_bucket(batch_string):
	"""Retrieve a dataset from the bucket and create a csv file of the collected facebook counts"""
	s3_bucket = S3Bucket()
	batch_s3_folder = 'data/{timestamp}'.format(timestamp=batch_string)
	try:
		response = s3_bucket.get('{folder}/store_{timestamp}.json'.format(folder=batch_s3_folder, timestamp=batch_string))
	except s3_bucket.client.exceptions.NoSuchKey:
		try:
			response = s3_bucket.get('{folder}/reach_{timestamp}.json'.format(folder=batch_s3_folder, timestamp=batch_string))
		except s3_bucket.client.exceptions.NoSuchKey:
			# TODO should not use exceptions for this expected alternative
			try:
				response = s3_bucket.get('{folder}/reach.json'.format(folder=batch_s3_folder))
			except s3_bucket.client.exceptions.NoSuchKey:
				logger.warning('Cannot find data store for {date}'.format(date=batch_string))
				return

	estimates = json.loads(response['Body'].read())

	preprocess_counts(batch_string, estimates)

	counts_csv_filename = 'counts_{timestamp}.csv'.format(timestamp=batch_string)
	counts_csv_filepath = os.path.join(data_path, counts_csv_filename)

	with open(counts_csv_filepath, 'rb') as countfile:
		key = '{folder}/{file}'.format(folder=batch_s3_folder, file=counts_csv_filename)
		s3_bucket.put(key, countfile)


def preprocess_counts(batch_string, estimates):
	"""Blank any missing data or ratios and write the facebook counts csv"""
	ratios = {
		'FB_age_13_14_ratio': {'agerange': '13-14', 'men': 'FB_age_13_14_men', 'women': 'FB_age_13_14_women'},
		'FB_age_15_19_ratio': {'agerange': '15-19', 'men': 'FB_age_15_19_men', 'women': 'FB_age_15_19_women'},
		'FB_age_20_24_ratio': {'agerange': '20-24', 'men': 'FB_age_20_24_men', 'women': 'FB_age_20_24_women'},
		'FB_age_25_29_ratio': {'agerange': '25-29', 'men': 'FB_age_25_29_men', 'women': 'FB_age_25_29_women'},
		'FB_age_30_34_ratio': {'agerange': '30-34', 'men': 'FB_age_30_34_men', 'women': 'FB_age_30_34_women'},
		'FB_age_35_39_ratio': {'agerange': '35-39', 'men': 'FB_age_35_39_men', 'women': 'FB_age_35_39_women'},
		'FB_age_40_44_ratio': {'agerange': '40-44', 'men': 'FB_age_40_44_men', 'women': 'FB_age_40_44_women'},
		'FB_age_45_49_ratio': {'agerange': '45-49', 'men': 'FB_age_45_49_men', 'women': 'FB_age_45_49_women'},
		'FB_age_50_54_ratio': {'agerange': '50-54', 'men': 'FB_age_50_54_men', 'women': 'FB_age_50_54_women'},
		'FB_age_55_59_ratio': {'agerange': '55-59', 'men': 'FB_age_55_59_men', 'women': 'FB_age_55_59_women'},
		'FB_age_60_64_ratio': {'agerange': '60-64', 'men': 'FB_age_60_64_men', 'women': 'FB_age_60_64_women'},
		'FB_age_18_23_ratio': {'agerange': '18-23', 'men': 'FB_age_18_23_men', 'women': 'FB_age_18_23_women'},
		'FB_age_20_plus_ratio': {'agerange': '20+', 'men': 'FB_age_20_plus_men', 'women': 'FB_age_20_plus_women'},
		'FB_age_20_64_ratio': {'agerange': '20-64', 'men': 'FB_age_20_64_men', 'women': 'FB_age_20_64_women'},
		'FB_age_21_plus_ratio': {'agerange': '21+', 'men': 'FB_age_21_plus_men', 'women': 'FB_age_21_plus_women'},
		'FB_age_25_plus_ratio': {'agerange': '25+', 'men': 'FB_age_25_plus_men', 'women': 'FB_age_25_plus_women'},
		'FB_age_25_49_ratio': {'agerange': '25-49', 'men': 'FB_age_25_49_men', 'women': 'FB_age_25_49_women'},
		'FB_age_25_64_ratio': {'agerange': '25-64', 'men': 'FB_age_25_64_men', 'women': 'FB_age_25_64_women'},
		'FB_age_50_plus_ratio': {'agerange': '50+', 'men': 'FB_age_50_plus_men', 'women': 'FB_age_50_plus_women'},
		'FB_age_60_plus_ratio': {'agerange': '60+', 'men': 'FB_age_60_plus_men', 'women': 'FB_age_60_plus_women'},
		'FB_age_65_plus_ratio': {'agerange': '65+', 'men': 'FB_age_65_plus_men', 'women': 'FB_age_65_plus_women'},
	}
	device_ratios = {
		'FB_android_device_users_ratio': 'All Android devices',
		'FB_iOS_device_users_ratio': 'All iOS Devices',
		'FB_mobile_device_users_ratio': 'All Mobile Devices',
		'FB_feature_phone_users_ratio': 'Feature Phone',
		'FB_iPhone7_users_ratio': 'iphone 7'
	}
	logger.info('Beginning preprocessing for analysis for {date}'.format(date=batch_string))
	blanking_value = ''

	counts_csv_filename = 'counts_{timestamp}.csv'.format(timestamp=batch_string)
	counts_csv_filepath = os.path.join(data_path, counts_csv_filename)
	try:
		os.remove(counts_csv_filepath)
	except OSError:
		pass

	with open(counts_csv_filepath, 'w', newline='') as csvfile:
		if estimates:
			writer = None

			for country_key in estimates:
				country = estimates[country_key]
				estimate = 'estimate_dau'
				row = {'Country': country_key}
				# TODO use estimate store
				try:
					row['FB_all'] = country['all']['18+'][estimate]

					row['FB_age_18_plus_men'] = country['men']['18+'][estimate]
					row['FB_age_18_plus_women'] = country['women']['18+'][estimate]
				except KeyError:
					logger.error("Missing critical population ratio in {country}. 18+ population ratio not collected".format(country=country_key))
					continue
				try:
					row['FB_age_18_plus_ratio'] = row['FB_age_18_plus_women'] / row['FB_age_18_plus_men']
				except ZeroDivisionError:
					logger.error("Missing critical population ratio in {country}. Male 18+ user count is 0. Dropping country from analysis".format(country=country_key))
					continue
				except KeyError:
					logger.error("Missing critical population ratio in {country}. 18+ population ratio not collected".format(country=country_key))
					continue
				if row['FB_age_18_plus_ratio'] == 0:
					logger.error("Missing critical population ratio in {country}. Female 18+ user count is 0. Dropping country from analysis".format(country=country_key))
					continue

				for ratio, ratiokeys in ratios.items():
					try:
						row[ratiokeys['men']] = country['men'][ratiokeys['agerange']][estimate]
						row[ratiokeys['women']] = country['women'][ratiokeys['agerange']][estimate]
					except KeyError:
						logger.warning("Ratio problem in {country}, {key} counts uncollected".format(country=country_key, key=ratio))
						row[ratio] = blanking_value
					try:
						row[ratio] = row[ratiokeys['women']] / row[ratiokeys['men']]
					except ZeroDivisionError:
						logger.warning("Ratio problem in {country}, {key} count is 0".format(country=country_key, key=ratiokeys['men']))
						row[ratio] = blanking_value
					except KeyError:
						logger.warning("Ratio problem in {country}, {key} counts uncollected".format(country=country_key, key=ratio))
						row[ratio] = blanking_value
					if row[ratio] == 0:
						logger.warning("Ratio problem in {country}, {key} count is 0".format(country=country_key, key=ratiokeys['women']))
						row[ratio] = blanking_value

				for ratio, facebookkey in device_ratios.items():
					try:
						row[ratio] = country['women']['18+'][facebookkey][estimate] / country['men']['18+'][facebookkey][estimate]
					except ZeroDivisionError:
						logger.warning("Ratio problem in {country}, {key} male user count is 0".format(country=country_key, key=facebookkey))
						row[ratio] = blanking_value
					except KeyError:
						logger.warning("Ratio problem in {country}, {key} counts uncollected".format(country=country_key, key=ratio))
						row[ratio] = blanking_value
					if row[ratio] == 0:
						logger.warning("Ratio problem in {country}, {key} female user count is 0".format(country=country_key, key=facebookkey))
						row[ratio] = blanking_value

				try:
					row['FB_smartphone_owners_ratio'] = country['women']['18+']["SmartPhone Owners"][estimate] / country['men']['18+']["SmartPhone Owners"][estimate]
				except ZeroDivisionError:
					logger.warning("Ratio problem in {country}, {key} male user count is 0".format(country=country_key, key='smartphones and tablets'))
					row['FB_smartphone_owners_ratio'] = blanking_value
				except KeyError:
					try:
						female_smartphone_owners = country['women']['18+']["Facebook access (mobile): smartphones and tablets"][estimate] - country['women']['18+']["Facebook access (mobile): tablets"][estimate]
						male_smartphone_owners = country['men']['18+']["Facebook access (mobile): smartphones and tablets"][estimate] - country['men']['18+']["Facebook access (mobile): tablets"][estimate]
						row['FB_smartphone_owners_ratio'] = female_smartphone_owners / male_smartphone_owners
					except ZeroDivisionError:
						logger.warning("Ratio problem in {country}, {key} male user count is 0".format(country=country_key, key='smartphones and tablets'))
						row['FB_smartphone_owners_ratio'] = blanking_value
					except KeyError:
						logger.warning("Ratio problem in {country}, {key} counts uncollected".format(country=country_key, key="FB_smartphone_owners_ratio"))
						row['FB_smartphone_owners_ratio'] = blanking_value
					if row['FB_smartphone_owners_ratio'] == 0:
						logger.warning("Ratio problem in {country}, {key} female user count is 0".format(country=country_key, key='smartphones and tablets'))
						row['FB_smartphone_owners_ratio'] = blanking_value
				if row['FB_smartphone_owners_ratio'] == 0:
					logger.warning("Ratio problem in {country}, {key} female user count is 0".format(country=country_key, key='smartphones and tablets'))
					row['FB_smartphone_owners_ratio'] = blanking_value

				if not writer:
					writer = csv.DictWriter(csvfile, fieldnames=row.keys())
					writer.writeheader()
				writer.writerow(row)


def merge_counts_with_offline_dataset(batch_string):
	"""Merge the facebook counts csv with the offline dataset csv"""
	s3_bucket = S3Bucket()
	batch_s3_folder = 'data/{timestamp}'.format(timestamp=batch_string)
	counts_csv_filename = 'counts_{timestamp}.csv'.format(timestamp=batch_string)
	counts_csv_filepath = os.path.join(data_path, counts_csv_filename)
	dataset_csv_filepath = os.path.join(data_path, 'Digital_Gender_Gap_Dataset.csv')

	try:
		os.remove(dataset_csv_filepath)
	except OSError:
		pass

	with open(os.path.join(data_path, 'Digital_gender_gap_dataset_updated_ITU_data.csv'), 'r') as datafile, open(counts_csv_filepath, 'r') as countfile, open(dataset_csv_filepath, 'w', newline='') as outfile:
		data_reader = csv.DictReader(datafile)
		data = list(data_reader)
		# data = {rows[0]:rows[1] for rows in data_reader}
		count_reader = csv.DictReader(countfile)
		data_out = []
		for count in count_reader:
			id = count['Country']
			isoalpha3 = None
			if len(id) == 2:
				with open(os.path.join(data_path, 'countries.json'), 'r') as countryfile:
					country_lookup = json.load(countryfile)
					if id in country_lookup:
						isoalpha3 = country_lookup[id][2]
					else:
						logger.warning('Country {code} missing from lookup file'.format(code=id))
						#TODO XK is Kosovo
			elif len(id) == 3:
				isoalpha3 = id
			row = next((country for country in data if country['ISO3Code'] == isoalpha3), None)
			if row:
				pop_ratio = float(count['FB_age_18_plus_ratio'])
				if pop_ratio < 0.15:
					logger.warning("Dropped country {iso}, population ratio too low. {ratio}".format(iso=count['Country'], ratio=pop_ratio))
				elif pop_ratio > 1.85:
					logger.warning("Dropped country {iso}, population ratio too high. {ratio}".format(iso=count['Country'], ratio=pop_ratio))
				else:
					for key in row:
						if key in count and key != 'Country':
							row[key] = count[key]
					data_out.append(row)
			else:
				logger.warning("Dropped country {iso}, can't find a matching country in the offline dataset".format(iso=count['Country']))

		writer = csv.DictWriter(outfile, fieldnames=data_out[0].keys())
		writer.writeheader()
		writer.writerows(data_out)

	with open(dataset_csv_filepath, 'rb') as countfile:
		remote_dataset_csv_filename = 'Digital_Gender_Gap_Dataset_{timestamp}.csv'.format(timestamp=batch_string)
		key = '{folder}/{file}'.format(folder=batch_s3_folder, file=remote_dataset_csv_filename)
		s3_bucket.put(key, countfile)


def preprocess_analysis_data(batch_string):
	"""Create the facebook counts csv from a bucket dataset and then merge with the offline dataset csv"""
	preprocess_counts_from_bucket(batch_string)
	merge_counts_with_offline_dataset(batch_string)
