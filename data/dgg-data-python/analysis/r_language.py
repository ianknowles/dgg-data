"""Classes for interacting with the R language"""
import os
import subprocess

from dgg_log import root_logger

logger = root_logger.getChild(__name__)


class RExecutable:
	"""Represents the R script executable in a local install. Capable of running R language scripts"""
	def __init__(self):
		# TODO find the install path, or just place in config file?
		self.filepath = 'C:\\Program Files\\R\\R-3.5.0\\bin\\Rscript.exe'
		if not os.path.isfile(self.filepath):
			logger.error(f'Cannot find R executable {self.filepath}')
			raise FileNotFoundError

	def run_script(self, script_filepath):
		"""Run the given R script with the R executable and log its output"""
		try:
			logger.info(f'Running R script {os.path.basename(script_filepath)}')
			# TODO RECHECK
			p = subprocess.Popen([self.filepath, os.path.basename(script_filepath)], cwd=os.path.dirname(script_filepath), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
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
