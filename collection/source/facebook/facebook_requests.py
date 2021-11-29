"""Classes and functions for representing Facebook Marketing API requests and targeting data"""
import copy
import csv
import datetime
import json
import os
from functools import reduce

from storage.dgg_file_structure import data_path


def _reducer(items, key, val, pref, sep):
	newkey = key
	if pref:
		newkey = f"{pref}{sep}{key}"
	if isinstance(val, dict):
		#print(f"dict with key {newkey}")
		return {**items, **flatten(val, newkey)}
	elif isinstance(val, list):
		L = []
		#print(f"list with key {key}")
		#print(val)
		for x, new_val in enumerate(val):
			if isinstance(new_val, list) or isinstance(new_val, dict):
				L.append(flatten(new_val, f"{newkey}{sep}{x}"))
			else:
				L.append({**items, f"{newkey}{sep}{x}": new_val})
		return {**items, **{k: v for d in L for k, v in d.items()}}
	else:
		#print(f"any with key {newkey}")
		return {**items, newkey: val}

def flatten(d, pref='', sep='-'):
	return(reduce(
		lambda new_d, kv: _reducer(new_d, *kv, pref, sep),
		d.items(),
		{}
	))

class FacebookReachRequest:
	"""Class representing Facebook Marketing API requests"""
	def __init__(self, params):
		self.params = copy.deepcopy(params)
		self.response = None
		self.completed = False
		self.valid = False
		self.attempts = 0
		self.timestamp = None
		self.futures = None
		self.responses = []

	def complete(self):
		"""Mark the request as completed and record the timestamp"""
		self.timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp()
		self.completed = True

	def to_dict(self):
		return {
			'params': self.params,
			#'response': self.response,
			'completed': self.completed,
			'valid': self.valid,
			'attempts': self.attempts,
			'timestamp': self.timestamp,
			#'responses': [r.export_all_data() for r in self.responses],
			'responses': self.responses
		}

	def to_flat_dict(self):
		return flatten(self.to_dict())

age_ranges = [
	{"age_min": 18},
	{"age_min": 13, "age_max": 14},
	{"age_min": 14, "age_max": 15},
	{"age_min": 15, "age_max": 16},
	{"age_min": 16, "age_max": 17},
	{"age_min": 17, "age_max": 18},
	{"age_min": 18, "age_max": 19},
	{"age_min": 15, "age_max": 19},
	{"age_min": 20, "age_max": 24},
	{"age_min": 25, "age_max": 29},
	{"age_min": 30, "age_max": 34},
	{"age_min": 35, "age_max": 39},
	{"age_min": 40, "age_max": 44},
	{"age_min": 45, "age_max": 49},
	{"age_min": 50, "age_max": 54},
	{"age_min": 55, "age_max": 59},
	{"age_min": 60, "age_max": 64},
	{"age_min": 65},
	{"age_min": 18, "age_max": 23},
	{"age_min": 20},
	{"age_min": 20, "age_max": 64},
	{"age_min": 21},
	{"age_min": 25},
	{"age_min": 25, "age_max": 49},
	{"age_min": 25, "age_max": 64},
	{"age_min": 50},
	{"age_min": 60}
]
behaviors_default = [
	{"id": 6002714898572, "name": "Small business owners"},
	{"id": 6017253486583, "name": "Facebook access (network type): 2G"},
	{"id": 6017253511583, "name": "Facebook access (network type): 3G"},
	{"id": 6017253531383, "name": "Facebook access (network type): 4G"},
	{"id": 6015235495383, "name": "Facebook access (network type): Wi-Fi"},
	{"id": 6091658707783, "name": "Uses a mobile device (less than 1 month)"},
	{"id": 6091658708183, "name": "Uses a mobile device (1-3 months)"},
	{"id": 6091658512983, "name": "Uses a mobile device (4-6 months)"},
	{"id": 6091658512183, "name": "Uses a mobile device (7-9 months)"},
	{"id": 6091658540583, "name": "Uses a mobile device (10-12 months)"},
	{"id": 6091658562383, "name": "Uses a mobile device (13-18 months)"},
	{"id": 6091658651583, "name": "Uses a mobile device (19-24 months)"},
	{"id": 6091658683183, "name": "Uses a mobile device (25 months+)"},
	{"id": 6004384041172, "name": "Facebook access (mobile): Apple (iOS )devices"},
	{"id": 6004386044572, "name": "Facebook access (mobile): Android devices"},
	{"id": 6004383149972, "name": "Facebook access (mobile): feature phones"},
	{"id": 6004382299972, "name": "Facebook access (mobile): all mobile devices"},
	{"id": 6004383049972, "name": "Facebook access (mobile): smartphones and tablets"},
	{"id": 6016286626383, "name": "Facebook access (mobile): tablets"},
	{"id": 6060616578383, "name": "iphone 7"},
	{"id": 6015547900583, "name": "Facebook access (browser): Chrome"},
	{"id": 6015593608983, "name": "Facebook access (browser): Safari"},
	{"id": 6015547847583, "name": "Facebook access (browser): Firefox"},
	{"id": 6055133998183, "name": "Facebook access (browser): Microsoft Edge"},
	{"id": 6015593776783, "name": "Facebook access (browser): Internet Explorer"},
	{"id": 6015593652183, "name": "Facebook access (browser): Opera"},
	{"id": 6015559470583, "name": "Lives abroad"},
]

# 'interests'
behaviors_parents = [
	{"id": 6002991239659, "name": "Motherhood"},
	{"id": 6003101323797, "name": "Fatherhood"}
]

family_status = [{"id": 6023005372383, "name": "New parents (0-12 months)"}]


def create_country_target_queue(alpha2, behaviors=family_status):
	"""Creates a queue of FacebookReachRequest for the given country using the standard dgg targeting lists"""
	targeting_spec = {
		'geo_locations': {
			'countries': [alpha2],
			#'countries': ['GB'],
			#'medium_geo_areas': [{'key': alpha2}],
			'location_types': ['home'],
		},
		'publisher_platforms': ["instagram"] # instagram, audience_network
	}
	# targeting_spec["facebook_positions"] = ["feed"]
	# targeting_spec["device_platforms"] = ["mobile","desktop"]
	params = {
		'targeting_spec': targeting_spec,
		# TODO is the opt goal correct?
		'optimization_goal': "AD_RECALL_LIFT"  # Not none or reach?
	}
	params['targeting_spec']['age_min'] = 18
	queue = []
	queue += [FacebookReachRequest(params)]
	for behavior in behaviors:
		params['targeting_spec']['family_statuses'] = [behavior]
		queue += [FacebookReachRequest(params)]

	for gender in range(1, 3):
		targeting_spec['genders'] = [gender]
		targeting_spec.pop('behaviors', None)
		targeting_spec.pop('interests', None)
		targeting_spec.pop('family_statuses', None)
		for age in age_ranges:
			targeting_spec.pop('age_min', None)
			targeting_spec.pop('age_max', None)
			targeting_spec.pop('family_statuses', None)
			params['targeting_spec'] = {**targeting_spec, **age}
			queue += [FacebookReachRequest(params)]
			for behavior in behaviors:
				params['targeting_spec']['family_statuses'] = [behavior]
				queue += [FacebookReachRequest(params)]
		params['targeting_spec']['age_min'] = 18
		targeting_spec.pop('age_max', None)
		targeting_spec.pop('family_statuses', None)
		for behavior in behaviors:
			params['targeting_spec']['family_statuses'] = [behavior]
			queue += [FacebookReachRequest(params)]
	return queue


def populations(locations):
	queue = []
	for location in locations:
		params = {
			'targeting_spec': {
				'geo_locations': {
					#'countries': [location['country_code']],
					#handle diff regions
					'cities': [{'key': location['key']}],
					'location_types': ['home'],
				},
				'publisher_platforms': ["facebook"]
			},
			'optimization_goal': "AD_RECALL_LIFT"  # Not none or reach?
		}
		queue += [FacebookReachRequest(params)]
	return queue


def create_request_queue(locations):
	queue = []
	genders = range(1, 3)

	for location in locations:
		location_plural = ""
		if location['type'] == 'city':
			location_plural = 'cities'
		elif location['type'] == 'region':
			location_plural = 'regions'
		params = {
			'targeting_spec': {
				'geo_locations': {
					#'countries': [location['country_code']],
					location_plural: [{'key': location['key']}],
					'location_types': ['home'],
				},
				'publisher_platforms': ["facebook"]
			},
			'optimization_goal': "AD_RECALL_LIFT"  # Not none or reach?
		}
		queue += [FacebookReachRequest(params)]
		for gender in genders:
			params = {
				'targeting_spec': {
					'geo_locations': {
						#'countries': [location['country_code']],
						#handle diff regions
						location_plural: [{'key': location['key']}],
						'location_types': ['home'],
					},
					'genders': [gender],
					'publisher_platforms': ["facebook"]
				},
				'optimization_goal': "AD_RECALL_LIFT"  # Not none or reach?
			}
			queue += [FacebookReachRequest(params)]
			for age in age_ranges:
				params = {
					'targeting_spec': {**age,
						'geo_locations': {
							#'countries': [location['country_code']],
							#handle diff regions
							location_plural: [{'key': location['key']}],
							'location_types': ['home'],
						},
						'publisher_platforms': ["facebook"],
						'genders': [gender],
					},
					'optimization_goal': "AD_RECALL_LIFT"  # Not none or reach?
				}
				queue += [FacebookReachRequest(params)]

			for behavior in behaviors_default:
				params = {
					'targeting_spec': {
						'geo_locations': {
							#'countries': [location['country_code']],
							#handle diff regions
							location_plural: [{'key': location['key']}],
							'location_types': ['home'],
						},
						'publisher_platforms': ["facebook"],
						'genders': [gender],
						'behaviors': [behavior],
					},
					'optimization_goal': "AD_RECALL_LIFT"  # Not none or reach?
				}
				queue += [FacebookReachRequest(params)]

	return queue


if __name__ == "__main__":
	#total_countries = 246
	#queue = create_country_target_queue('US')
	#queue_size = len(queue)
	#print('QLen= {queue_size}'.format(queue_size=queue_size))
	#print('Estimated queue run time')
	#time = datetime.timedelta(seconds=(queue_size * total_countries * 0.67))
	#print(time)
	#print('Estimated queue run time')
	#time = datetime.timedelta(seconds=(1320 * total_countries * 0.67))
	#print(time)
	with open(os.path.join(data_path, "2021-08-17_AF_regions_collection.json"), "r") as file:
		collection = json.load(file)
		rows = [flatten(r) for r in collection['complete']]
		fieldnames = {}
		for r in rows:
		#print(r)
			if r is not None:
		#print(r.keys())
				fieldnames.update(r)

		with open(os.path.join(data_path, "2021-08-17_AF_regions_collection.csv"), 'w', encoding='utf8', newline='') as file:
			writer = csv.DictWriter(file, fieldnames=list(fieldnames))

			writer.writeheader()
			writer.writerows(rows)
