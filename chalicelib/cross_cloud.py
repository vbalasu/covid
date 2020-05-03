def gcs_upload(file, bucket_name, key):
  import os
  os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.realpath('chalicelib/covid19-sales-engineering-1379-1af154282a84.json')
  from google.cloud import storage
  client = storage.Client()
  bucket = client.get_bucket(bucket_name)
  blob = storage.Blob(key, bucket)
  blob.upload_from_filename(file)

def gcs_download(bucket_name, key, file):
  import os
  os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.realpath('chalicelib/covid19-sales-engineering-1379-1af154282a84.json')
  from google.cloud import storage
  client = storage.Client()
  bucket = client.get_bucket(bucket_name)
  blob = storage.Blob(key, bucket)
  blob.download_to_filename(file)

def s3_to_gcs(source_bucket, target_bucket, key):
  import uuid, boto3
  s3 = boto3.client('s3')
  file = '/tmp/'+str(uuid.uuid4())
  s3.download_file(source_bucket, key, file)
  gcs_upload(file, target_bucket, key)

def gcs_to_s3(source_bucket, target_bucket, key):
  import uuid, boto3
  s3 = boto3.client('s3')
  file = '/tmp/'+str(uuid.uuid4())
  gcs_download(source_bucket, key, file)
  s3.upload_file(file, target_bucket, key)
