"""
Lunio Audit History S3 Bucket Data Processing

This script retrieves files from the 'poc-audit-history-records' S3 bucket and processes the data into a readable CSV file.

Usage:
- Ensure that you have valid AWS credentials configured on your system.
- Run the script with account_id parameter and aw_id parameter (optional) to process the files and generate a CSV file.

Required Python Packages:
- boto3
- pandas

Note:
- This script assumes you have read only access to the bucket on production. Unknown errors may be thrown if not.

"""

import argparse
import boto3
import os
import shutil
import gzip
import pandas as pd
import io
import json
from datetime import datetime, timedelta

bucket_name = "poc-audit-history-records"
folder_path = "tmp"

def aw_id_target(args):
	prefix = f"aw_id/account_id={args.account_id}/adwords_id={args.aw_id}"

	success, client, objects = bucket_rip(prefix, bucket_name, folder_path, args)
	if not success:
		return False

	if len(objects) == 0:
		print("No items found for account_id and aw_id.")
		return False

	i = 0
	for item in objects:
		i += 1
		local_file_path = f"{folder_path}/{item['Key'].split('/')[-1]}"
		client.download_file(bucket_name, item['Key'], local_file_path)
		print(f"Downloaded {i} of {len(objects)}.", end="\r")

	print("Download finished. Starting parsing data.")

	df = process_gz_files(folder_path)

	if os.path.exists(folder_path):
		shutil.rmtree(folder_path)

	output_file_name = f"{args.account_id}-{args.aw_id}-{datetime.now()}.csv"
	df.to_csv(output_file_name, index=False)

	return True


def account_id_target(args):
	prefix = f"timestamp/account_id={args.account_id}/"

	success, client, objects = bucket_rip(prefix, bucket_name, folder_path, args)
	if not success:
		return False

	i = 0
	for item in objects:
		i += 1
		local_file_path = f"{folder_path}/{item['Key'].replace('/', '-')}"
		client.download_file(bucket_name, item['Key'], local_file_path)
		print(f"Downloaded {i} of {len(objects)}.", end="\r")

	print("Download finished. Starting parsing data.")

	df = process_gz_files(folder_path)

	if os.path.exists(folder_path):
		shutil.rmtree(folder_path)

	output_file_name = f"{args.account_id}-{args.orig_timestamp}-{datetime.now()}.csv"
	df.to_csv(output_file_name, index=False)

	return True


def bucket_rip(prefix, bucket_name, folder_path, args):
	try:
		# Make sure user has boto3, otherwise provide a user friendly error message.
		client = boto3.client("s3")
	except Exception as e:
		print(e)
		print("Error instantiating boto3 client. Please run 'pip install boto3' before re-running this script.")
		return False, None, None

	try:
		paginator = client.get_paginator("list_objects_v2")
		objects = []

		if args.aw_id:
			pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
			for page in pages:
				print(page)
				if "Contents" in page.keys():
					objects.append(page['Contents'])
		else:
			end_date = datetime.now()
			while args.timestamp <= end_date:
				args.timestamp += timedelta(days=1)
				new_prefix = f"{prefix}date={args.timestamp.strftime('%Y-%m-%d')}/"
				pages = paginator.paginate(Bucket=bucket_name, Prefix=new_prefix)
				for page in pages:
					if "Contents" in page.keys():
						objects.append(page['Contents'])

		objects = [item for sublist in objects for item in sublist]

	except Exception as e:
		print(e)
		print("Error connecting to aws. Please make sure your credentials are up to date.")
		return False, None, None

	try:
		if os.path.exists(folder_path):
			shutil.rmtree(folder_path)
		os.makedirs(folder_path)
	except Exception as e:
		print(e)
		print("Failed to create temp folder.")
		return False, None, None

	return True, client, objects


def process_gz_files(path):
	file_list = os.listdir(path)
	data_list = []

	for file_name in file_list:
	    file_path = os.path.join(path, file_name)

	    if file_name.endswith('.gz'):
	        with gzip.open(file_path, 'rt') as file:
	            file_contents = file.read()
	            file_contents = file_contents.split('\n')
	            for i in range(len(file_contents)):
	            	file_contents[i] = json.loads(file_contents[i])

	        df = pd.DataFrame(file_contents)
        	data_list.append(df)

	combined_df = pd.concat(data_list, ignore_index=True)
	return combined_df


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Lunio Audit History Parser")
	parser.add_argument(
		"--account_id",
		required=True,
		help="Account ID value of the account being searched"
	)

	parser.add_argument(
		"--aw_id",
		help="Customer or campaign ID"
	)

	parser.add_argument(
		"--timestamp",
		help="Date (YYYY-MM-DD) of the oldest record you want to pull."
	)

	args = parser.parse_args()

	if args.timestamp is None:
		# If no timestamp is provided when required, set max date to 30 days ago.
		args.timestamp = datetime.now() - timedelta(days=30)
	else:
		args.timestamp = datetime.strptime(args.timestamp, "%Y-%m-%d")

	args.orig_timestamp = args.timestamp.strftime('%Y-%m-%d')

	if args.aw_id is not None:
		success = aw_id_target(args)
	else:
		success = account_id_target(args)
	
	message = "Script finished without issue." if success else "An error occurred and the script failed to complete"
	print(f"--- {message} ---")
