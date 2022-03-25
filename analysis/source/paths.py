"""Module path definitions setting out the local folder structure"""
import os

file_path = os.path.dirname(os.path.realpath(__file__))
module_path = os.path.normpath(os.path.join(file_path, '..'))

log_path = os.path.join(module_path, 'logs')
auth_path = os.path.join(module_path, 'auth')
input_path = os.path.join(module_path, 'input')
count_path = os.path.join(input_path, 'counts')
output_path = os.path.join(module_path, 'output')
r_path = os.path.join(module_path, 'source')
