
import boto3
import sys
import os
import constants
import utils
import csv
import json
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError
import logging


# Constants
INPUT_BUCKET_NAME = constants.INPUT_BUCKET_NAME
OUTPUT_BUCKET_NAME = constants.OUTPUT_BUCKET_NAME
# Bucket with CSV files to download and upload to input bucket
BUCKET_WITH_FILES = constants.BUCKET_WITH_FILES
LAB_BUCKET_DATA_FILE_KEYS = constants.LAB_BUCKET_DATA_FILE_KEYS
STUDENT_BUCKET_DATA_FILE_KEYS = constants.STUDENT_BUCKET_DATA_FILE_KEYS

class DataTransform:
    s3 = None
    def __init__(self):
        self.INPUT_BUCKET_NAME = INPUT_BUCKET_NAME
        self.OUTPUT_BUCKET_NAME = OUTPUT_BUCKET_NAME

        # Set the region in which the buckets are created
        self.BUCKET_REGION = boto3.session.Session().region_name

        # Create a s3 resource
        self.s3 = boto3.resource('s3')

        # Set up the input bucket and copy the CSV files.
        utils.setup_bucket(self.s3, self.INPUT_BUCKET_NAME, self.BUCKET_REGION)
        # Download csv files
        utils.download_csv_files()
        # Copy csv files to Input bucket
        utils.upload_csv_files_input_bucket(self.s3, self.INPUT_BUCKET_NAME)
        # delete local csv files
        utils.delete_local_files('.txt')

        # Set up the output bucket
        utils.setup_bucket(self.s3, self.OUTPUT_BUCKET_NAME, self.BUCKET_REGION)

        # S3 input bucket
        inputBucket = self.s3.Bucket(self.INPUT_BUCKET_NAME)
        # S3 output bucket
        outputBucket = self.s3.Bucket(self.OUTPUT_BUCKET_NAME)

         # Get summary information for all objects in input bucket
        # Iterate over the list of object summaries
        for objSummary in inputBucket.objects.all():
            # get the obj key from obj summary
            csvKey = objSummary.key

            # retrieve the obj with the specified key from input bucket
            utils.download_file_from_bucket(inputBucket, csvKey)

            # transform downloaded csv files to json files
            [jsonKey, jsonName] = self.transform(csvKey)

            # upload transformed JSON file to output bucket
            outputBucket.upload_file(jsonName, jsonName)
            print('Uploaded file {0} to {1} outputBucket'.format(jsonName, outputBucket))

            # generate presigned url for transformed files
            # client = boto3.client('s3')
            url = self.s3.meta.client.generate_presigned_url('get_object', 
                Params={'Bucket': self.OUTPUT_BUCKET_NAME, 'Key': jsonName},
                ExpiresIn=900,
                HttpMethod='GET')
            print('presigned url is generated for {0}: {1}'.format(jsonName, url))

            





    def transform(self, file):
        f = open(file, 'r')
        print('Open file {0} to read'.format(file))

        fieldNames = f.readlines(1)[0].split(',')
        reader = csv.DictReader(f, fieldNames)
        # Convert to json
        out = json.dumps([row for row in reader])
        f.close()

        # Store JSON in a file
        jsonKey = file.split('.')[0]
        jsonName = file.split('.')[0]+'.json'
        jsonFile = open(jsonName, 'w')
        jsonFile.write(out)
        print('Created Json file {0} from CSV file {1}'.format(jsonFile, file))

        return [jsonKey, jsonName]

        






if __name__ == "__main__":
    DataTransform()

    # utils.deleteBuckets([INPUT_BUCKET_NAME, OUTPUT_BUCKET_NAME])
