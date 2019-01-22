"""Classes and functions for representing Facebook Marketing API requests and targeting data"""
import copy
import time


class FacebookReachRequest:
	"""Class representing Facebook Marketing API requests"""
	def __init__(self, params):
		self.params = copy.deepcopy(params)
		self.response = None
		self.completed = False
		self.valid = False
		self.attempts = 0
		self.timestamp = None

	def complete(self):
		"""Mark the request as completed and record the timestamp"""
		self.timestamp = time.time()
		self.completed = True


age_ranges = [
	{"age_min": 18},
	{"age_min": 13, "age_max": 14},
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
behaviors = [
	{
		"name": "All Mobile Devices",
		"id": 6004382299972
	},

	{
		"name": "All Android devices",
		"id": 6004386044572
	},

	{
		"name": "All iOS Devices",
		"id": 6004384041172
	},

	{
		"name": "Feature Phone",
		"id": 6004383149972
	},

	{
		"name": "iphone 7",
		"id": 6060616578383
	},

	{
		"name": "Facebook access (mobile): tablets",
		"id": 6016286626383
	},

	{
		"name": "Facebook access (mobile): smartphones and tablets",
		"id": 6004383049972
	}
]


def create_country_target_queue(alpha2):
	"""Creates a queue of FacebookReachRequest for the given country using the standard dgg targeting lists"""
	targeting_spec = {
		'geo_locations': {
			'countries': [alpha2],
			'location_types': ['home'],
		},
		'publisher_platforms': ["facebook"]
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
		params['targeting_spec']['behaviors'] = [behavior]
		queue += [FacebookReachRequest(params)]

	for gender in range(1, 3):
		targeting_spec['genders'] = [gender]
		targeting_spec.pop('behaviors', None)
		for age in age_ranges:
			targeting_spec.pop('age_min', None)
			targeting_spec.pop('age_max', None)
			params['targeting_spec'] = {**targeting_spec, **age}
			queue += [FacebookReachRequest(params)]
		params['targeting_spec']['age_min'] = 18
		targeting_spec.pop('age_max', None)
		for behavior in behaviors:
			params['targeting_spec']['behaviors'] = [behavior]
			queue += [FacebookReachRequest(params)]
	return queue
