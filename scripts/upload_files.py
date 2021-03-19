#!/usr/bin/env python3

# Purpose: Upload Synthea patient data files
# Author:  Gary A. Stafford (March 2021)

import logging
import pathlib

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)

ssm_client = boto3.client('ssm')
s3_resource = boto3.resource('s3')


def main():
    params = get_parameters()
    data_bucket = params['data_bucket']
    upload_file(data_bucket)


def upload_file(data_bucket):
    """Upload a file to an S3 object"""

    try:
        file_path = 'synthea_data/'
        current_directory = pathlib.Path(file_path)
        for current_file in current_directory.iterdir():
            s3_resource.meta.client.upload_file(
                str(current_file), data_bucket, str(current_file))
            logging.info(f'File upload completed: {current_file.name.replace(file_path, "")} ({round(current_file.stat().st_size / 1024 / 1024, 2)} MB)')
    except ClientError as e:
        logging.error(e)
        exit(1)


def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    params = {
        'data_bucket': ssm_client.get_parameter(
            Name='/macie_demo/patient_data_bucket')['Parameter']['Value']
    }

    return params


if __name__ == '__main__':
    main()
