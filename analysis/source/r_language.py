"""Classes for interacting with the R language"""
import os
import subprocess
import sys

from dgg_log import root_logger

logger = root_logger.getChild(__name__)


class RExecutable:
	"""Represents the R script executable in a local install. Capable of running R language scripts"""
	def __init__(self, filepath=''):
		# TODO check standard env variables, R_HOME?
		self.filepath = filepath
		if not os.path.isfile(self.filepath):
			if sys.platform.startswith('win32'):
				# TODO try other versions in the R folder
				self.filepath = 'C:\\Program Files\\R\\R-3.5.0\\bin\\Rscript.exe'
			elif sys.platform.startswith('linux'):
				self.filepath = '/usr/lib/R/bin/Rscript'
			if os.path.isfile(self.filepath):
				logger.info(f'Found R executable {self.filepath}')
			else:
				logger.error(f'Cannot find R executable {self.filepath}')
				raise FileNotFoundError

	def run_script(self, script_filepath, args):
		"""Run the given R script with the R executable and log its output"""
		try:
			logger.info(f'Running R script {os.path.basename(script_filepath)}')
			# TODO RECHECK
			p = subprocess.Popen([self.filepath, script_filepath] + args, cwd=os.path.dirname(script_filepath), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
			with p.stdout:
				for line in iter(p.stdout.readline, b''):
					decoded_line = line.decode('utf-8').strip('\n\r')
					logger.info(decoded_line)
			p.wait()
		except subprocess.CalledProcessError:
			logger.error('Error reported from the R script executable')
			raise SystemExit
		except FileNotFoundError:
			logger.error('Cannot find R executable')
			raise SystemExit
