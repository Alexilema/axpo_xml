# -*- coding: utf-8 -*-
"""
Created on Mon Jun 23 16:30:14 2025

@author: alex_
"""

# Libraries
import xmltodict
import datetime as dt
import os
import pandas as pd
# import sqlite3

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
input_path = config['Paths']['input_path']
func_path  = config['Paths']['func_path']

# Import auxiliar script containing .xml parsing functions
path.append(func_path)
# from xml_parser_func import xml_parser

#%%
# Option 2: if the files are in a S3 bucket
# ...

#%%
# Split files in provisional and final lists
files_prov = []
files_fin = []
files = []

    # for filename in os.listdir(input_path):
        
    #     # Keep track of the file full path
    #     file = os.path.join(input_path, filename)
        
    #     # Check data type
    #     if filename.endswith('.xml'):
    #         if filename.startswith('provisional'):
    #             # Attach the full file path, not just the filename
    #             files_prov.append(file)
            
    #         if filename.startswith('final'):
    #             files_fin.append(file)
        
for filename in os.listdir(input_path):
    file = os.path.join(input_path, filename)
    files.append(file)

# @@ PRUEBAS
# file_key = files_prov[0]

## Function to parse the .xml files
def xml_parser(file_key):
    try:
        # Read file content as a JSON dict
        with open(file_key) as file:
            # Read file content as str (text)
            xml_data = file.read()
            
        # As that text already has the xml estructure, it can be directly
        # transformed a json dict (xmltodict helps us do that straight away)
        data = xmltodict.parse(xml_data) # (**)
        
        # (**) Note that, as our files contain multiples elements with the same
        # tag "Reading", maintaining the original order is neccesary to keep
        # the correct "Sequence". Fortunately, dicts items already do that
        
        # Empty dict to "flatten" the original structure, so that we can later
        # turn it into a table (for example, a pandas df, which we will then
        # load into a SQLite DB)
        file_dict = {}
        
        # Read root element
        meter_data = data.get('MeterData')
        
        if meter_data:
            # Store main level atribute values in the new dict
            file_dict.update({
                'MeterPointId':      meter_data.get('MeterPointId'),
                'FromTimestamp':     meter_data.get('FromTimestamp'),
                'ToTimestamp':       meter_data.get('ToTimestamp'),
                'FlowDirection':     meter_data.get('FlowDirection'),
                'Resolution':        meter_data.get('Resolution'),
                'Unit':              meter_data.get('Unit'),
                'CreationTimestamp': meter_data.get('CreationTimestamp'),
                'DataType':          meter_data.get('DataType')
                })
            
            # Move onto the next sublevel
            reading_dict = meter_data.get('ReadingList', {})
            
            # arg {} means if the key is not found, it returns an empty dict
            
            if reading_dict:
                # All the "Reading" elements are grouped in just one element
                # shaped as a dict of 1 key ("Reading") and 1 value of 
                # list containing all the Sequence items
                reading_list = reading_dict.get('Reading', {})
                
                # If there is only one Reading element, it won't return a list,
                # therefore to avoid errors we turn it into one
                if not isinstance(reading_list, list):
                    if isinstance(reading_list, dict): # check dict type
                        reading_list = [reading_list]  # turn into a list
                    else:
                        raise ValueError('Something is wrong with the "Reading" element of the file')
    
                for reading in reading_list:
                    # Get sequence order
                    i = reading.get('Sequence')
                    
                    # Store values
                    file_dict[f'Readings_{i}_Value']   = reading.get('Value')
                    file_dict[f'Readings_{i}_Quality'] = reading.get('Quality')
        
        # Return flattened dict
        return file_dict
    
    # If the parsing of the .xml file fails, the function returns nothing
    except Exception as e:
        print(f'Failed to parse file {file_key}.\n Error: ', e)
        return None
        
#%%
dict_list = []

# Parse each file
for file_key in files:
    
    file_dict = xml_parser(file_key)
    
    # Attach it to the list
    dict_list.append(file_dict)
    
# Turn into a df (the keys are the columns, and each dict corresponds to one row)
df = pd.DataFrame(dict_list)

#%%
# Data cleansing

