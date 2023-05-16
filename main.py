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
from datetime import datetime


def aw_id_target(args):
	bucket_name = "poc-audit-history-records"
	folder_path = "tmp"
	prefix = f"aw_id/account_id={args.account_id}/adwords_id={args.aw_id}"

	success, client, objects = bucket_rip(prefix, bucket_name, folder_path)
	if not success:
		return False

	i = 0
	for item in objects['Contents']:
		i += 1
		local_file_path = f"{folder_path}/{item['Key'].split('/')[-1]}"
		client.download_file(bucket_name, item['Key'], local_file_path)
		print(f"Downloaded {i} of {len(objects['Contents'])}.", end="\r")

	print("Download finished. Starting parsing data.")

	df = process_gz_files(folder_path)
	print(df)

	if os.path.exists(folder_path):
		shutil.rmtree(folder_path)

	output_file_name = f"{args.account_id}-{args.aw_id}-{datetime.now()}.csv"
	df.to_csv(output_file_name, index=False)

	return True

def account_id_target(args):
	print("This ins't comeplete")
	return True

def bucket_rip(prefix, bucket_name, folder_path):
	try:
		# Make sure user has boto3, otherwise provide a user friendly error message.
		client = boto3.client("s3")
	except Exception as e:
		print(e)
		print("Error instantiating boto3 client. Please run 'pip install boto3' before re-running this script.")
		return False, None, None

	try:
		objects = client.list_objects_v2(
			Bucket=bucket_name,
			Prefix=prefix
		)
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
	parser = argparse.ArgumentParser(description="Jobs main code")
	parser.add_argument(
		"--account_id",
		required=True,
		help="Account ID value of the account being searched"
	)

	parser.add_argument(
		"--aw_id",
		help="Customer or campaign ID"
	)

	args = parser.parse_args()
	if args.aw_id is not None:
		success = aw_id_target(args)
	else:
		success = account_id_target(args)
	
	message = "End" if success else "An error occurred and the script failed to complete"
	print(f"--- {message} ---")
