#!/usr/bin/python

import simplejson, boto3, sys, time
# enable below for debugging
#boto3.set_stream_logger(name='botocore')
from datetime import datetime, timedelta

expires = datetime.utcnow() + timedelta(days=(365 + 30))

s3 = boto3.client('s3')
s3rs = boto3.resource('s3')

paginator = s3.get_paginator('list_objects')

bucket = 'sentia-tmp'
prefix = 'arc/'

# iterate through all objects in this bucket with a specific prefix (=path)
for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page['Contents']:
                key = obj['Key']
                # if a key ends with a /, it's a directory, so we don't need to process it
                if not key.endswith("/"):
                        key_obj = s3rs.Object(bucket, key)
                        print key_obj.key, key_obj.content_type, key_obj.metadata
                        # we need to save the content_Type and metadata because it will get lost
                        content_type = key_obj.content_type
                        metadata = key_obj.metadata
                        # the only way to change the metadata for a S3 object is to make a copy of it
                        s3.copy_object(Bucket=bucket, CopySource=bucket+'/'+key, Expires=expires, Key=key, MetadataDirective='REPLACE', ContentType=content_type, Metadata=metadata)
                        # we will set the ACL to public-read as this gets reset to private with the copy process
                        s3.put_object_acl(Bucket=bucket, Key=key, ACL='public-read')