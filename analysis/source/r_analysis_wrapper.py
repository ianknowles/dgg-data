"""Methods for running the R analysis script. Run this script to analyse today's collection"""
import datetime
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


def predict_from_file(script_filepath, input_filepath, output_path):
	cleanup_r_script_outputs(output_path)

	logger.info('Beginning analysis')
	r_exe = r_language.RExecutable()
	r_exe.run_script(os.path.join(script_filepath, "Digital_gender_gaps_analysis_updated_7Nov2019.R"), [input_filepath, output_path])
	logger.info('Analysis complete')

	return {'predictions': os.path.join(output_path, 'Appendix_table_model_predictions.csv'), 'fits': os.path.join(output_path, 'fits.csv')}


def predict(batch_string, estimate='mau'):
	"""Run an analysis for the given day. Inputs are expected to be retrievable from S3"""
	preprocess_analysis_data(batch_string, estimate)

	s3_bucket = S3Bucket()

	files = predict_from_file(r_path, f'{estimate}_counts_{batch_string}.csv', data_path)

	batch_s3_folder = f'data/{batch_string}'
	key = ''
	with open(files['predictions'], 'rb') as file:
		filename = f'{estimate}_monthly_model_2_{batch_string}.csv'
		key = f'{batch_s3_folder}/{filename}'
		s3_bucket.put(key, file)
		logger.info(f'Uploaded {filename}')

	with open(files['fits'], 'rb') as file:
		filename = f'{estimate}_monthly_model_2_{batch_string}_fits.csv'
		fit_key = f'{batch_s3_folder}/{filename}'
		s3_bucket.put(fit_key, file)
		logger.info(f'Uploaded {filename}')
	return key


if __name__ == "__main__":
	from storage.dgg_file_structure import log_path
	logging_setup(log_path)


	today_string = str(datetime.date.today().isoformat())
	predict(today_string)
	predict(today_string, 'dau')
