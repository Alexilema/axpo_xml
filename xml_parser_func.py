# -*- coding: utf-8 -*-
"""
Created on Tue Jun 24 23:15:05 2025

@author: alex_
"""

# Libraries
import xmltodict


# Function to parse each .xml file
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
        # tag 'Reading', maintaining the original order is neccesary to keep
        # the correct 'Sequence'. Fortunately, dicts items already do that
        
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
                # All the 'Reading' elements are grouped in just one element
                # shaped as a dict of 1 key ('Reading') and 1 value of 
                # list containing all the Sequence items
                reading_list = reading_dict.get('Reading', {})
                
                # If there is only one Reading element, it won't return a list,
                # therefore to avoid errors we turn it into one
                if not isinstance(reading_list, list):
                    if isinstance(reading_list, dict): # check dict type
                        reading_list = [reading_list]  # turn into a list
                    else:
                        raise ValueError('Something is wrong with the "Reading" element of the file')
    
                for i, reading in enumerate(reading_list):
                    # We must differenciate between i and 'Sequence' value to
                    # be able to check if there are gaps in the sequence
                    
                    # Python indexes start on 0, the 'Sequence' starts at 1, so they must be matched
                    i += 1 
                    
                    # Store values
                    file_dict[f'Readings_{i}_Sequence'] = reading.get('Sequence')
                    file_dict[f'Readings_{i}_Value']    = reading.get('Value')
                    file_dict[f'Readings_{i}_Quality']  = reading.get('Quality')
        
        # Return flattened dict
        return file_dict
    
    # If the parsing of the .xml file fails, the function returns nothing
    except Exception as e:
        print(f'Failed to parse file {file_key}.\n Error: ', e)
        return None
