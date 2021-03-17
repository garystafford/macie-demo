#!/usr/bin/env python3

# Purpose: Create 1x Macie classification job - Synthea patient data
# Author:  Gary A. Stafford (March 2021)

import logging
import time

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)

ssm_client = boto3.client('ssm')
sts_client = boto3.client('sts')
macie_client = boto3.client('macie2')


def main():
    params = get_parameters()
    account_id = sts_client.get_caller_identity()['Account']
    custom_data_identifiers = list_custom_data_identifiers()
    create_classification_job(params['patient_data_bucket'], account_id, custom_data_identifiers)


def list_custom_data_identifiers():
    """Returns a list of all custom data identifier ids"""

    custom_data_identifiers = []

    try:
        response = macie_client.list_custom_data_identifiers()
        for item in response['items']:
            custom_data_identifiers.append(item['id'])
    except ClientError as e:
        logging.error(e)
    finally:
        return custom_data_identifiers


def create_classification_job(patient_data_bucket, account_id, custom_data_identifiers):
    """Create 1x Macie classification job"""

    try:
        response = macie_client.create_classification_job(
            customDataIdentifierIds=custom_data_identifiers,
            description='Review Synthea patient data (1x)',
            jobType='ONE_TIME',
            initialRun=True,
            name=f'SyntheaPatientData_{int(time.time())}',
            s3JobDefinition={
                'bucketDefinitions': [
                    {
                        'accountId': account_id,
                        'buckets': [
                            patient_data_bucket
                        ]
                    }
                ],
                'scoping': {
                    'includes': {
                        'and': [
                            {
                                'simpleScopeTerm': {
                                    'comparator': 'EQ',
                                    'key': 'OBJECT_EXTENSION',
                                    'values': [
                                        'csv',
                                    ]
                                }
                            },
                        ]
                    }
                }
            },
            tags={
                'Project': 'Amazon Macie Demo'
            }
        )
        print(f'Response: {response}')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    params = {
        'patient_data_bucket': ssm_client.get_parameter(Name='/macie_demo/patient_data_bucket')['Parameter']['Value']
    }

    return params


if __name__ == '__main__':
    main()
