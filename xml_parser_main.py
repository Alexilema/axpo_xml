# -*- coding: utf-8 -*-
"""
Created on Mon Jun 23 16:30:14 2025

@author: alex_
"""

# Libraries
import datetime as dt
import os
import pandas as pd
import sqlite3
from tqdm import tqdm
import traceback

from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
from pathlib import Path
from sys import path

#%%
# Read input .xml files

# Option 1: if the files are in a local folder

# Read .cfg con los paths local
ACT_PATH = str(Path(__file__).parent.resolve()) # (*)

# (*) Note that this line fails when executed line by line, whole cell or
# script must be executed

config = ConfigParser()
config.read(ACT_PATH + '/params.cfg', encoding='utf-8')

# Path to the folder containing the input .xml files
input_path  = config['Paths']['input_path']
func_path   = config['Paths']['func_path']
output_path = config['Paths']['output_path']

# Import auxiliar script containing the parsing function
path.append(func_path)
from xml_parser_func import xml_parser

# Option 2: if the files are in a remote storage (like an S3 bucket)
# --> Check code in "xml_parser_extra.py"

#%%
# Apply parsing function

# List all .xml files
xml_files = [
    os.path.join(input_path, filename)
    for filename in os.listdir(input_path)
    if filename.endswith(".xml")
]
# Could also use os.walk() ...

# Initialize empty list to gather the data from all files
# dict_list = []
#
# # Option 1. Parse each file (classic method)
# for file_key in xml_files:
#     file_dict = xml_parser(file_key)
#     # Attach it to the list
#     dict_list.append(file_dict)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# However, as we expect to process ~5000 files at once, we can speed up the
# process by using parallel computation. There are 2 options:
#   - locally: using the computer threads (multithreading)
#   - on cloud: using tools like Spark (PySpark)
# Note: both can actually be used combined!

# Option 2. Parallel parsing using Multithreading
try:
    with ThreadPoolExecutor(max_workers=4) as executor:
        # max_workers is the maximum number of parallel tasks
        # (should not exceed the number of CPU cores)
        
        # Initialize list to gather the data
        dict_list = []
        
        # Initialize list to store the parallel tasks to be performed
        futures = []
        
        # Use tqdm to monitor progress
        with tqdm(desc="Parsing", total=len(xml_files)) as pbar:
            
            # Submit a task for each file
            for file_key in xml_files:
                future = executor.submit(xml_parser, file_key)
                futures.append(future)
            
            # Perform the tasks
            for future in futures:
                try:
                    result = future.result()
                    
                    # Add results only if they are not None
                    if result:
                        dict_list.append(result)
                    pbar.update(1)
                
                except Exception as e:
                    print(f"Error while parsing {file_key}: {e}")  

except Exception:
    print(traceback.format_exc())
    raise

# Final step: turn the gathered data into a df (the keys are the columns, and
# each dict (each file) corresponds to one row)
df = pd.DataFrame(dict_list)

# Note: if any of the dictionaries in dict_list is missing some keys (some columns),
# it doesn't matter, pandas handles that on its own, including all different keys
# found as new columns, and matching existing keys even if they don't come in the
# same order in all the dicts. Basically, pandas makes our life easy here :)

# Note: once the 5.000 files have been turned into a 5.000 rows df, the rest of 
# the processing does not need enhanced computing, as it is a very small df and
# therefore pandas is fast enough

#%%
# Datetime types fixing

# Convert datetime cols to datetime type
for col in df.columns:
    if 'timestamp' in col.lower():
        
        # Cast to datetimem, coercing errors into NaTs
        df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Note: UTC+02:00 means these values are Local Time datetimes
        
        # If a value has datetime format, but the UTC zone is missing, replace
        # it with NaT
        df[col] = df[col].apply(
            lambda x:
                x if pd.isnull(x) or x.tzinfo is not None
                else pd.NaT
            )

        # Transform datetime values from UTC+2 to UTC
        df[col] = df[col].dt.tz_convert("UTC")
        
        # Note that this mixes up dates in each file (could be relevant when
        # designing the validation)
        
#%%
# Data Validation

# STEP 1: each file must be exacty one day

# Create a mask to select only the rows (files) whose timespans are exactly 24 hours
mask_time = df['ToTimestamp'] - df['FromTimestamp'] == dt.timedelta(hours=24)

# Drop invalid rows
df = df[mask_time]

# STEP 2: Numeric values must contain exactly 2 decimals
# Note: if a value ends in zero, turning it into numeric would lose that info
# Instead, let's make the validation first with values as str, and later we will
# turn them to type numeric (optional)

def check_decimals(value):
    # (Optional) Make sure it is type str
    value = str(value)
    
    # If no decimals, invalid value
    if '.' not in value:
        return False
    
    decimals = value.split('.')[-1]
    
    # If it has 2 decimals, valid value
    if len(decimals) == 2:
        return True
    else:
        return False

# Check each numeric column
for col in df.columns:
    if 'value' in col.lower():
        
        # Create a mask based on the check function
        mask_decimals = df[col].apply(check_decimals)
        
        # Filter valid values
        df = df[mask_decimals]
        
        # (Optional) Cast to numeric
        # - float as we need decimals
        # - if float64 is not needed, we could use 'float32' for memory optimization
        # - coerce errors to NAs (we could use drop instead, but I for now let's keep track of errors)
        df[col] = pd.to_numeric(df[col], errors='coerce')  # .astype('float32')

# STEP 3: Sequence must start from 1 and go incrementally with no gaps

def check_sequence(row):
    return

for col in df.columns:
    if 'sequence' in col.lower():
        # Select the col number
        col_num = col.split('_')[1]
        
        # Compare that number to the original 'Sequence' value
        # If they differ, drop that row, for the sequence is compromised
        mask_seq = df[col] == col_num
        
        # Filter valid values
        df = df[mask_seq]

#%%
# SQLite DB

# Make the output dir if needed
if not os.path.isdir(output_path):
    os.mkdir(output_path)

# Specify DB path
file_db = os.path.join(output_path, 'meter_data.db')
# Note: we could use other extensions such as .sqlite or .dat

# Connect to the DB
conn = sqlite3.connect(file_db)

# Note: this line creates the DB if it doesn't already exists

# Insert the processed data in the sqlite DB
df.to_sql(name='MeterData',  # table name inside de DB
          con=conn,
          if_exists='append',
          index=False        # ignore the df index
          )

print('Data succesfully added to the DB')

#%%
# # DEV_test: check that is worked!

# conn = sqlite3.connect(file_db)

# df_check = pd.read_sql_query("SELECT * FROM MeterData LIMIT 5;", conn)
# print(df_check)

# conn.close()



