import calendar
import csv
import datetime
import os
import statistics

import pycountry

from preprocessing import preprocess_counts
from r_analysis_wrapper import predict_from_file
from analysis_index import ModelIndexFile

from dgg_log import logging_setup, root_logger
from paths import count_path, r_path, output_path, log_path, auth_path
from storage.S3_bucket import S3Bucket


logger = root_logger.getChild(__name__)

s3_auth = os.path.join(auth_path, 'S3_keys.json')

csv_columns = {
	"FB_all": {'age_group': '18+', 'gender': 'all', 'behavior': ''},
	'FB_age_18_plus_men': {'age_group': '18+', 'gender': 'men', 'behavior': ''},
	"FB_age_18_plus_women": {'age_group': '18+', 'gender': 'women', 'behavior': ''},
	"FB_age_13_14_men": {'age_group': '13-14', 'gender': 'men', 'behavior': ''},
	"FB_age_13_14_women": {'age_group': '13-14', 'gender': 'women', 'behavior': ''},
	"FB_age_15_19_men": {'age_group': '15-19', 'gender': 'men', 'behavior': ''},
	"FB_age_15_19_women": {'age_group': '15-19', 'gender': 'women', 'behavior': ''},
	"FB_age_20_24_men": {'age_group': '20-24', 'gender': 'men', 'behavior': ''},
	"FB_age_20_24_women": {'age_group': '20-24', 'gender': 'women', 'behavior': ''},
	"FB_age_25_29_men": {'age_group': '25-29', 'gender': 'men', 'behavior': ''},
	"FB_age_25_29_women": {'age_group': '25-29', 'gender': 'women', 'behavior': ''},
	"FB_age_30_34_men": {'age_group': '30-34', 'gender': 'men', 'behavior': ''},
	"FB_age_30_34_women": {'age_group': '30-34', 'gender': 'women', 'behavior': ''},
	"FB_age_35_39_men": {'age_group': '35-39', 'gender': 'men', 'behavior': ''},
	"FB_age_35_39_women": {'age_group': '35-39', 'gender': 'women', 'behavior': ''},
	"FB_age_40_44_men": {'age_group': '40-44', 'gender': 'men', 'behavior': ''},
	"FB_age_40_44_women": {'age_group': '40-44', 'gender': 'women', 'behavior': ''},
	"FB_age_45_49_men": {'age_group': '45-49', 'gender': 'men', 'behavior': ''},
	"FB_age_45_49_women": {'age_group': '45-49', 'gender': 'women', 'behavior': ''},
	"FB_age_50_54_men": {'age_group': '50-54', 'gender': 'men', 'behavior': ''},
	"FB_age_50_54_women": {'age_group': '50-54', 'gender': 'women', 'behavior': ''},
	"FB_age_55_59_men": {'age_group': '55-59', 'gender': 'men', 'behavior': ''},
	"FB_age_55_59_women": {'age_group': '55-59', 'gender': 'women', 'behavior': ''},
	"FB_age_60_64_men": {'age_group': '60-64', 'gender': 'men', 'behavior': ''},
	"FB_age_60_64_women": {'age_group': '60-64', 'gender': 'women', 'behavior': ''},
	"FB_age_18_23_men": {'age_group': '18-23', 'gender': 'men', 'behavior': ''},
	"FB_age_18_23_women": {'age_group': '18-23', 'gender': 'women', 'behavior': ''},
	"FB_age_20_plus_men": {'age_group': '20+', 'gender': 'men', 'behavior': ''},
	"FB_age_20_plus_women": {'age_group': '20+', 'gender': 'women', 'behavior': ''},
	"FB_age_20_64_men": {'age_group': '20-64', 'gender': 'men', 'behavior': ''},
	"FB_age_20_64_women": {'age_group': '20-64', 'gender': 'women', 'behavior': ''},
	"FB_age_21_plus_men": {'age_group': '21+', 'gender': 'men', 'behavior': ''},
	"FB_age_21_plus_women": {'age_group': '21+', 'gender': 'women', 'behavior': ''},
	"FB_age_25_plus_men": {'age_group': '25+', 'gender': 'men', 'behavior': ''},
	"FB_age_25_plus_women": {'age_group': '25+', 'gender': 'women', 'behavior': ''},
	"FB_age_25_49_men": {'age_group': '25-49', 'gender': 'men', 'behavior': ''},
	"FB_age_25_49_women": {'age_group': '25-49', 'gender': 'women', 'behavior': ''},
	"FB_age_25_64_men": {'age_group': '25-64', 'gender': 'men', 'behavior': ''},
	"FB_age_25_64_women": {'age_group': '25-64', 'gender': 'women', 'behavior': ''},
	"FB_age_50_plus_men": {'age_group': '50+', 'gender': 'men', 'behavior': ''},
	"FB_age_50_plus_women": {'age_group': '50+', 'gender': 'women', 'behavior': ''},
	"FB_age_60_plus_men": {'age_group': '60+', 'gender': 'men', 'behavior': ''},
	"FB_age_60_plus_women": {'age_group': '60+', 'gender': 'women', 'behavior': ''},
	"FB_age_65_plus_men": {'age_group': '65+', 'gender': 'men', 'behavior': ''},
	"FB_age_65_plus_women": {'age_group': '65+', 'gender': 'women', 'behavior': ''},

	# Older collections may have slightly different key for these columns e.g. FB_android_device_users_ratio_women
	"FB_android_device_users_women": {'age_group': '18+', 'gender': 'women', 'behavior': 'All Android devices'},
	"FB_android_device_users_men": {'age_group': '18+', 'gender': 'men', 'behavior': 'All Android devices'},
	"FB_iOS_device_users_women": {'age_group': '18+', 'gender': 'women', 'behavior': 'All iOS Devices'},
	"FB_iOS_device_users_men": {'age_group': '18+', 'gender': 'men', 'behavior': 'All iOS Devices'},
	"FB_mobile_device_users_women": {'age_group': '18+', 'gender': 'women', 'behavior': 'All Mobile Devices'},
	"FB_mobile_device_users_men": {'age_group': '18+', 'gender': 'men', 'behavior': 'All Mobile Devices'},
	"FB_feature_phone_users_women": {'age_group': '18+', 'gender': 'women', 'behavior': 'Feature Phone'},
	"FB_feature_phone_users_men": {'age_group': '18+', 'gender': 'men', 'behavior': 'Feature Phone'},
	"FB_iPhone7_users_women": {'age_group': '18+', 'gender': 'women', 'behavior': 'iphone 7'},
	"FB_iPhone7_users_men": {'age_group': '18+', 'gender': 'men', 'behavior': 'iphone 7'},

	"FB_smartphone_owners_ratio": {'age_group': '18+', 'gender': 'all', 'behavior': 'SmartPhone Owners'},
}

device_ratios = {
	'All Android devices',  # 'Facebook access (mobile): Android devices'
	'All iOS Devices',  # Facebook access (mobile): Apple (iOS )devices
	'All Mobile Devices',  # Facebook access (mobile): all mobile devices
	'Feature Phone',  # Facebook access (mobile): feature phones
	'iphone 7'
}


class MonthlyAnalysis:
	"""Monthly analysis"""
	def __init__(self, year, month, estimate='mau'):
		super().__init__()
		self.year = year
		self.month = month
		self.estimate = estimate

		self.start_date = datetime.date(self.year, self.month, 1)
		self.num_days = calendar.monthrange(self.year, self.month)[1]
		self.end_date = datetime.date(self.year, self.month, self.num_days)
		self.days_dates = [self.start_date + datetime.timedelta(days=x) for x in range(0, self.num_days)]

		self.month_datestamp = self.start_date.strftime("%Y-%m")
		self.count_filename = f'{self.estimate}_monthly_counts_{self.start_date.isoformat()}.csv'
		self.count_filepath = os.path.join(count_path, self.count_filename)
		self.counts_folder = os.path.join(count_path, self.month_datestamp)

		self.output_path = os.path.join(output_path, self.month_datestamp)
		self.prediction_filepath = ''
		self.fit_filepath = ''

		self.prediction_filename = f'{self.estimate}_monthly_model_2_{self.month_datestamp}.csv'
		self.fits_filename = f'{self.estimate}_monthly_model_2_{self.month_datestamp}_fits.csv'

		self.s3_counts_root_folder = 'sql_export'

	def analyse(self):
		self.get_bucket_counts()
		self.generate_monthly_averages()

		if not os.path.exists(self.output_path):
			os.makedirs(self.output_path)
		files = predict_from_file(r_path, self.count_filepath, self.output_path)
		self.prediction_filepath = files['predictions']
		self.fit_filepath = files['fits']

		# monthly_check(year, month, estimate)

	def get_bucket_counts(self):
		s3_bucket = S3Bucket(s3_auth)
		logger.info(f"Getting count files for '{self.month_datestamp}' from '{self.s3_counts_root_folder}'")

		for date in self.days_dates:
			try:
				date_filename = f'{self.estimate}_counts_{date.isoformat()}.csv'
				date_filepath = os.path.join(self.counts_folder, date_filename)
				date_key = f'{self.s3_counts_root_folder}/{date.isoformat()}/{date_filename}'
				response = s3_bucket.get(date_key)
				if not os.path.exists(self.counts_folder):
					os.makedirs(self.counts_folder)
				with open(date_filepath, 'wb') as file:
					logger.debug(response)
					logger.info(f"Saving '{date_key}' to '{date_filepath}'")
					file.write(response['Body'].read())
					logger.info(f"Saved '{date_key}' to '{date_filepath}'")
			except s3_bucket.client.exceptions.NoSuchKey:
				pass

	def generate_monthly_averages(self):
		logger.info(f"Generating monthly averages for '{self.month_datestamp}'")
		ages = ["18+",
		 "13-14",
		 "14-15",
		 "15-16",
		 "16-17",
		 "17-18",
		 "18-19",
		 "15-19",
		 "20-24",
		 "25-29",
		 "30-34",
		 "35-39",
		 "40-44",
		 "45-49",
		 "50-54",
		 "55-59",
		 "60-64",
		 "65+",
		 "18-23",
		 "20+",
		 "20-64",
		 "21+",
		 "25+",
		 "25-49",
		 "25-64",
		 "50+",
		 "60+"]
		monthly_data = {}
		# country['all']['18+'][estimate_key]
		for date in self.days_dates:
			date_filename = f'{self.estimate}_counts_{date.isoformat()}.csv'
			with open(os.path.join(self.counts_folder, date_filename), 'r') as file:
				csv_reader = csv.DictReader(file)
				for row in csv_reader:
					country = row['Country']
					if len(country) == 3:
						country = pycountry.countries.get(alpha_3=country).alpha_2
					if country not in monthly_data:
						monthly_data[country] = {}
					for gender in ['all', 'men', 'women']:
						if gender not in monthly_data[country]:
							monthly_data[country][gender] = {}
						for agegroup in ages:
							if agegroup not in monthly_data[country][gender]:
								monthly_data[country][gender][agegroup] = {}
								monthly_data[country][gender][agegroup]['estimate_mau'] = []
								if agegroup == '18+':
									for dataestimate in device_ratios:
										monthly_data[country][gender][agegroup][dataestimate] = {}
										monthly_data[country][gender][agegroup][dataestimate]['estimate_mau'] = []
					for column in row:
						if column in csv_columns:
							if row[column]:
								if csv_columns[column]['behavior']:
									monthly_data[country][csv_columns[column]['gender']][csv_columns[column]['age_group']][csv_columns[column]['behavior']]['estimate_mau'].append(row[column])
								else:
									monthly_data[country][csv_columns[column]['gender']][csv_columns[column]['age_group']]['estimate_mau'].append(row[column])
							else:
								logger.warning(f'Blank data for {date}/{country}/{column}')
		print(monthly_data)

		monthly_averages = {}
		for country in monthly_data:
			if country not in monthly_averages:
				monthly_averages[country] = {}
			for gender in monthly_data[country]:
				if gender not in monthly_averages[country]:
					monthly_averages[country][gender] = {}
				for agegroup in monthly_data[country][gender]:
					if agegroup not in monthly_averages[country][gender]:
						monthly_averages[country][gender][agegroup] = {}
					for dataestimate in monthly_data[country][gender][agegroup]:
						if (dataestimate == 'timestamp') or (dataestimate == 'age_min') or (dataestimate == 'age_max') or (dataestimate == 'estimate_dau') or (dataestimate == 'estimate_mau'):
							data_values = list(map(lambda x: float(x), monthly_data[country][gender][agegroup][dataestimate]))
							if len(data_values):
								if len(data_values) > 1:
									quants = statistics.quantiles(data_values, n=4, method='inclusive')
									q1 = quants[0]
									q3 = quants[-1]
									filter_start = q1 - 1.5 * (q3 - q1)
									filter_end = q3 + 1.5 * (q3 - q1)

									df = filter(lambda x: (x >= filter_start) and (x <= filter_end), data_values)
								else:
									df = data_values
								monthly_averages[country][gender][agegroup][dataestimate] = statistics.mean(df)
						else:
							if dataestimate not in monthly_averages[country][gender][agegroup]:
								monthly_averages[country][gender][agegroup][dataestimate] = {}
							for data in monthly_data[country][gender][agegroup][dataestimate]:
								data_values = list(map(lambda x: float(x), monthly_data[country][gender][agegroup][dataestimate][data]))
								if len(data_values):
									if len(data_values) > 1:
										quants = statistics.quantiles(data_values, n=4, method='inclusive')
										q1 = quants[0]
										q3 = quants[-1]
										filter_start = q1 - 1.5 * (q3 - q1)
										filter_end = q3 + 1.5 * (q3 - q1)

										df = filter(lambda x: (x >= filter_start) and (x <= filter_end), data_values)
									else:
										df = data_values
									logger.debug(f'{country}-{gender}-{agegroup}-{dataestimate}-{data}')
									monthly_averages[country][gender][agegroup][dataestimate][data] = statistics.mean(df)

		preprocess_counts(self.start_date.isoformat(), self.count_filepath, monthly_averages, self.estimate)


class MonthlyAnalysisBucket(MonthlyAnalysis):
	def __init__(self, year, month, estimate='mau',
					s3_root_folder='database_analyses', model_index='data/monthly_models.json'):
		super().__init__(year, month, estimate)

		self.s3_folder = f'{s3_root_folder}/{self.month_datestamp}'
		self.s3_model_predictions_key = f'{self.s3_folder}/{self.prediction_filename}'
		self.s3_fits_key = f'{self.s3_folder}/{self.fits_filename}'

		self.s3_model_index = model_index

	def analyse(self):
		super().analyse()
		self.upload_outputs()

	def upload_outputs(self):
		s3_bucket = S3Bucket(s3_auth)

		with open(self.prediction_filepath, 'rb') as file:
			s3_bucket.put(self.s3_model_predictions_key, file)
			index = ModelIndexFile(s3_bucket, self.s3_model_index)
			index.add_latest(self.month_datestamp, self.s3_model_predictions_key)
			index.sort()

		with open(self.fit_filepath, 'rb') as file:
			s3_bucket.put(self.s3_fits_key, file)


def monthly_analysis_task(year, month, estimate):
	logging_setup(log_path)

	MonthlyAnalysis(year, month, estimate).analyse()


if __name__ == "__main__":
	monthly_analysis_task(2022, 2, 'mau')
