
import boto3
import sys
import os
import constants
import utils
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
            key = objSummary.key

            # retrieve the obj with the specified key from input bucket
            obj = self.s3.Object(self.INPUT_BUCKET_NAME, key)


    



    def generate_presigned_url(self, bucket, key):
        """ Generate a presigned URL to share an S3 object
        bucket - S3 bucket where the file exists
        key - Key (path and filename) of the file
        """

        s3Client = self.s3.meta.client
        try:
            url = s3Client.generate_presigned_url('get_object', Params = {
                'Bucket': bucket,
                'Key': key
            },
            ExpiresIn=900)
        except ClientError as e:
            logging.error(e)
            return None

        return url

    



if __name__ == "__main__":
    DataTransform()

    # utils.deleteBuckets([INPUT_BUCKET_NAME, OUTPUT_BUCKET_NAME])
