"""Project path definitions setting out the local folder structure"""
import os

file_path = os.path.dirname(os.path.realpath(__file__))
project_path = os.path.normpath(os.path.join(file_path, '..', '..'))

auth_path = os.path.join(project_path, 'auth')
config_path = os.path.join(project_path, 'config')
r_path = os.path.join(project_path, 'dgg-data-r')

mount_path = os.path.abspath(os.path.join(os.sep, 'mnt', 'datastore'))
log_path = os.path.join(project_path, 'logs')
data_path = os.path.join(project_path, 'data')
