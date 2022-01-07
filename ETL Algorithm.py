import glob                         # this module helps in selecting files
import pandas as pd                 # this module helps in processing CSV files
import xml.etree.ElementTree as ET  # this module helps in processing XML files.
from datetime import datetime

tmpfile = "dealership_temp.tmp"               # file used to store all extracted data
logfile = "dealership_logfile.txt"            # all event logs will be stored in this file
targetfile = "dealership_transformed_data.csv"   # file where transformed data is stored

# Add the CSV extract function below
def csv_extract_function(file):
    data = pd.read_csv(file)
    return data

# Add the json extract function below
def json_extract_function(file):
    data = pd.read_json(file, lines=True)
    return data

# Add the XML extract function below, it is the same as the xml extract function above but the column names need to be renamed.
def xml_extract_function(file):
    data = pd.DataFrame(columns=['car_model','year_of_manufacture','price', 'fuel'])
    tree = ET.parse(file)
    root = tree.getroot()
    for car in root:
        car_model = car.find("car_model").text
        year_of_manufacture = int(car.find("year_of_manufacture").text)
        price = float(car.find("price").text)
        fuel = car.find("fuel").text
        data = data.append({"car_model":car_model, "year_of_manufacture":year_of_manufacture, "price":price, "fuel":fuel}, ignore_index=True)
    return data


def extract():
    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price',
                                           'fuel'])  # create an empty data frame to hold extracted data

    # process all csv files
    for csvfile in glob.glob("dealership_data/*.csv"):
        extracted_data = extracted_data.append(csv_extract_function(csvfile), ignore_index=True)

    # process all json files
    for jsonfile in glob.glob("dealership_data/*.json"):
        extracted_data = extracted_data.append(json_extract_function(jsonfile), ignore_index=True)

    # process all xml files
    for xmlfile in glob.glob("dealership_data/*.xml"):
        extracted_data = extracted_data.append(xml_extract_function(xmlfile), ignore_index=True)

    return extracted_data

def transform(data):
    data['price'] = round(data['price'], 2)
    return data

# Add the load function below
def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile)

# Add the log function below
def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y' # Hour-Minute-Second-Monthname-Day-Year.
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("dealership_logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')

# Final ETL pipeline call it to activate the ETL process
def ETL_pipeline():
    log("ETL Job Started")

    log("Extract phase Started")
    extracted_data = extract()
    log("Extract phase Ended")

    log("Transform phase Started")
    transformed_data = transform(extracted_data)
    log("Transform phase Ended")

    log("Load phase Started")
    load(targetfile, transformed_data)
    log("Load phase Ended")

    log("ETL Job Ended")