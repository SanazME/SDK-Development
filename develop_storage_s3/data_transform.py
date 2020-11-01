
import boto3
import sys
import os
import constants
import utils

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

        # Set up the output bucket
        utils.setup_bucket(self.s3, self.OUTPUT_BUCKET_NAME, self.BUCKET_REGION)

        # S3 input bucket
        inputBucket = self.s3.Bucket(self.INPUT_BUCKET_NAME)
        # S3 output bucket
        outputBucket = self.s3.Bucket(self.OUTPUT_BUCKET_NAME)

# UTILS #####################################

##########################################

if __name__ == "__main__":
    DataTransform()

    # utils.deleteBuckets([INPUT_BUCKET_NAME, OUTPUT_BUCKET_NAME])
