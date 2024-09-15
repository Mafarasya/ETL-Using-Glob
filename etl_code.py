import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 
import json

# Define Paths
log_file = "log_file.txt"
target_file = "transformed_data.csv"

# Extraction
# CSV
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

# JSON
def extract_from_json(file_to_process):
    with open(file_to_process, 'r') as file:
        data = json.load(file)  
    dataframe = pd.DataFrame(data)
    return dataframe

# XML - Using ElementTree
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = person.find("height").text
        weight = person.find("weight").text
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name": name, "height": height, "weight": weight}])], ignore_index=True)
    return dataframe

# Data Extraction
def extract():
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])

    # Process all CSV Files
    for csvfile in glob.glob("./Sources/*.csv"):
        extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index = True)

    # Process all JSON Files
    for jsonfile in glob.glob("./Sources/*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index = True)

    # Process all XML Files
    for xmlfile in glob.glob("./Sources/*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index = True)

    return extracted_data

# Transformation
def transform(data):
    '''Convert inches to meters and round off to two decimals 
    1 inch is 0.0254 meters '''
    data['height'] = pd.to_numeric(data['height'], errors='coerce')
    data['height'] = round(data.height * 0.0254,2)

    '''Convert pounds to kilograms and round off to two decimals 
    1 pound is 0.45359237 kilograms '''
    data['weight'] = pd.to_numeric(data['weight'], errors='coerce')
    data['weight'] = round(data.weight * 0.45359237,2)

    return data

# Loading and Logging
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ', ' + message + '\n')

### ETL
log_progress("ETL Job Started")

# Log the beginning of the Extraction Process
log_progress("Extract Phase Started")
extracted_data = extract()

# Log the completion of the Extraction Process
log_progress("Extract Phase Ended")

# Log the beginning of the Transformation Process
log_progress("Transformation Phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# Log the completion of the Transformation Process
log_progress("Transformation Phase Ended")

# Load Process
log_progress("Load Phase Started")
load_data(target_file, transformed_data)

# Log the completion of the Loading Process
log_progress("Load Phase Ended")

log_progress("ETL Job Ended")




