"""Functions for constructing and sending report emails"""
import json
import os
import smtplib

from email.message import EmailMessage
from email.headerregistry import Address
from email.headerregistry import Group

from storage.dgg_file_structure import auth_path
from storage.dgg_file_structure import config_path

email_recipients_filepath = os.path.join(config_path, 'report_recipients.json')
gmail_auth_filepath = os.path.join(auth_path, 'gmail_user.json')

# TODO add links to files from collection
# TODO add summary
# TODO report important warnings such as credential expiry
# TODO OO


def add_recipients(msg: EmailMessage):
	"""Add the recipients listed in the local config file to the email"""
	with open(email_recipients_filepath, 'r') as recipient_file:
		recipients = json.load(recipient_file)['recipients']
	msg['To'] = Group('Report Group', list(map(lambda x: Address(**x), recipients)))


def attach_file(msg: EmailMessage, filepath):
	"""Attach a local file to the email"""
	with open(filepath, 'rb') as fp:
		text_data = fp.read()
		msg.add_attachment(text_data, maintype='text', subtype='plain', filename=os.path.basename(filepath))


def send_message(msg: EmailMessage):
	"""Send the message via gmail using the credentials stored in the auth folder"""
	with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s, open(gmail_auth_filepath, 'r') as user_file:
		user_dict = json.load(user_file)
	msg['From'] = Address(user_dict['display'], addr_spec=user_dict['user'])
	s.login(user_dict['user'], user_dict['password'])
	s.send_message(msg)


def add_errors_summary(body, store_filepath):
	"""Add a summary of any errors during collection to the email body"""
	# TODO update error summary reporting
	body += "Collection Error Summary\n"

	with open(store_filepath, 'r') as jsonfile:
		reach = json.load(jsonfile)
		errors = False
		for country in reach:
			if reach[country]['errors'] > 1:
				errors = True
				body += "\t{n} errors in collection of {country}\n".format(n=reach[country]['errors'], country=country)
			elif reach[country]['errors'] == 1:
				errors = True
				body += "\t{n} error in collection of {country}\n".format(n=reach[country]['errors'], country=country)
		if not errors:
			body += "\tNo collection errors"


def send_log(log_filepath, batch_string):
	"""Send an email for the given collection, and attach its log file"""
	# TODO exception handling, log file, results, or recipients could be missing

	msg = EmailMessage()
	msg['Subject'] = 'Collection Report'

	add_recipients(msg)

	msg.preamble = os.path.basename(log_filepath)

	body = "Log attached for collection {batch}".format(batch=batch_string)
	body += "\n\n"

	# add_errors_summary(body, store_filepath)

	msg.set_content(body)

	attach_file(msg, log_filepath)
	send_message(msg)


def send_error_log(log_filepath, batch_string):
	"""Send an email indicating a failure for the given collection, and attach its log file"""
	# TODO exception handling, log file, results, or recipients could be missing

	# Create the container email message.
	msg = EmailMessage()
	msg['Subject'] = 'Collection Failure Alert'

	add_recipients(msg)

	msg.preamble = os.path.basename(log_filepath)

	body = "Today's collection has failed".format(batch=batch_string)
	body += "\n\n"
	body += "Log attached for collection {batch}".format(batch=batch_string)
	body += "\n\n"

	# add_errors_summary(body, store_filepath)

	msg.set_content(body)

	attach_file(msg, log_filepath)
	send_message(msg)
