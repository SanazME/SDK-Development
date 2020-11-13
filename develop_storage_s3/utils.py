import boto3
import sys
import os
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError
import constants

def setup_bucket(s3, bucket, region):
    # if bucket exists: check permission and delete objects
    # if not then create a new one

    try:
        # Check if a bucket exists and you have permission to bucket
        s3.meta.client.head_bucket(
            Bucket=bucket,
        )
        # delete any objects
        s3.Bucket(bucket).objects.delete()

    except NoCredentialsError as e:
        print("Error: {0} {1}".format(
            e.response['Error']['Code'], e.response['Error']['Message']))

        sys.exit()

    except ClientError as e:
        errorCode = int(e.response['Error']['Code'])

        if errorCode == 404:
            # Bucket does not exist, create
            s3.create_bucket(Bucket=bucket)

            print('Created bucket: {0}'.format(bucket))

        else:
            print('Print a unique bucket name. Error: {0} {1}'.format(
                e.response['Error']['Code'], e.response['Error']['Message']))

            sys.exit()


def download_csv_files():
    s3labbucket = boto3.resource('s3', 'us-west-2')
    try:
        s3labbucket.meta.client.head_bucket(Bucket=constants.BUCKET_WITH_FILES)
    except NoCredentialsError as e:
        print("Invalid credentials")
        sys.exit()

    for index1 in range(len(constants.STUDENT_BUCKET_DATA_FILE_KEYS)):
        name = constants.LAB_BUCKET_DATA_FILE_KEYS[index1]
        key = constants.STUDENT_BUCKET_DATA_FILE_KEYS[index1]
        s3labbucket.Bucket(constants.BUCKET_WITH_FILES).download_file(name, key)
        print('Downloaded file ' + key)

def download_file_from_bucket(bucket, key):
    bucket.download_file(key, key)
    print('Downloaded file '+ key)


def upload_csv_files_input_bucket(s3, bucket):
    current_dir = os.getcwd()
    txt_files = find_same_type_files(current_dir, '.txt')

    for file in txt_files:
        try:
            s3.Bucket(bucket).upload_file(file, file)
        except NoCredentialsError as e:
            print('Invalid credentials')
            sys.exit()
        except ClientError as e:
            print(e)
            print('Unable to upload files')

        print('Uploaded file '+ file)


def find_same_type_files(dir, type):
    files = []
    for file in os.listdir(dir):
        if file.endswith(type):
            files.append(file)
    return files

def delete_local_files(type):
    files = find_same_type_files(os.getcwd(), type)
    for f in files:
        os.remove(f)
        print('{0} is removed!'.format(f))

def deleteBuckets(bucketList):
    s3 = boto3.resource('s3')
    for bucket in bucketList:
        try:
            s3.Bucket(bucket).objects.delete()
            s3.Bucket(bucket).delete()
        except NoCredentialsError as e:
                print('Invalid credentials')
                sys.exit()
        except ClientError as e:
            print(e)
            print('Unable to upload files')

        print('Bucket is deleted: ', bucket)
