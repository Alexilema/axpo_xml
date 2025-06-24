# -*- coding: utf-8 -*-
"""
Created on Tue Jun 24 23:37:13 2025

@author: alex_

This file contains extra code fragments (not ready to run) showcasing the 
of tools such as S3 buckets and PySpark in the taks


Note: I have not created a S3 Bucket for this task, as it would incur in cost and 
I do not think is neccesary for the purpose of this test. The code works 100% :)

Therefore, the execution of this cell will fail. This code is displayed just
as and example of how to get the files from S3
"""

# Option 2: if the files are in a remote storage (like an S3 bucket)

# Libraries
import datetime as dt
import os
from boto3.session import Session

# S3 access info
access_key = os.getenv('access_key_S3')
secret_key = os.getenv('secret_key_S3')

# Note: as a good practice, access info should not be written
# directly on the script. Instead, there are different ways of
# passing that info into the code execution. For example:
#   - configure the credentials on the enviroment (running "aws configure")
#   - use the params.cfg file as we did before with the paths
#   - create windows enviroment and/or system variables and load
#     them using os library and os.getenv() (as shown above)

# Fictional s3 bucket name
bucket_name = 'sample-bucket-name'

# Fictional s3 prefix, assuming we are already parsing the files on a
# daily basis and that each day folder name has format YYYY-MM-DD
current_date = dt.datetime.now().date()
s3_prefix = f'root_folder/{current_date}/'

# Create S3 session
session = Session(aws_access_key_id = access_key,
                  aws_secret_access_key = secret_key)

s3_client = session.client('s3')

# The usual s3_client.list_objects_v2() method can only list up to 1000 items
# As we are expecting up to 5000 files per day, we need you use paginator

# List all xml files from S3 with a paginator
paginator = s3_client.get_paginator('list_objects_v2')

xml_files = [
    obj['Key']  # key is the full s3 path of each element
    for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix)
    # each element has an associated page including its content and metadata
    if 'Contents' in page  # files do have Contents, not just metadata
    for obj in page['Contents']
    if obj['Key'].endswith('.xml') # select .xml files only
]

if not xml_files:
    raise ValueError(f"No files found in the specified S3 prefix {s3_prefix}")

# This code generates the xml files list in S3
# Then, we would have to modify the function script "RE_xml_parser_func.py"
# so that it would read the files from S3

# An S3 file key can be read easily like this:

# - pass the s3_client and the bucket_name onto the funcion as input args
# - replace the "with open read" code with:
#     response_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
#     xml_data = response_obj['Body'].read().decode('utf-8')
    
# - then the function stays the same:
#     data = xmltodict.parse(xml_data)
#     etc...
