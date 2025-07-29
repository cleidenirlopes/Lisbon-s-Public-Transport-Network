
# Lisbon's Public Transport Network Analysis

#In this project, I built a real-time data pipeline to monitor public bus statuses using a live API. The goal was to simulate an end-to-end data workflow — from raw data collection to interactive dashboards.

#Python collects and cleans real-time bus data from a public API (e.g., “In Transit”, “Stationed”).

#Cleaned data is sent to SQL Server, which acts as the central storage.

#SQL is used for hypothesis testing and querying traffic patterns.

#Power BI connects to SQL and displays a live dashboard.

#The refresh button in Power BI re-triggers the pipeline:
#Python pulls → SQL updates → Power BI refreshes.

#🔧 Key Skills Demonstrated
#API integration & real-time data handling

#Data cleaning and transformation with Python

#SQL for analysis & database management

#Power BI dashboard creation

#End-to-end automation mindset

#This project shows not just visualization — but a working pipeline from data ingestion to insights.
#1. **Data Collection**
 #  - Real-time bus positions
 # - Metro line status monitoring

#2. **Analysis Features**
 #  - Geographic visualization
 #  - District-based analysis
 #  - Status monitoring
 #  - Performance metrics
## 1. Setup and Dependencies
# Lisbon's Public Transport Network Analysis


## 1. Setup and Dependencies
#First, let's import all the required libraries for our analysis
import requests
import json
import pandas as pd
from tabulate import tabulate
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import pyodbc
import gtfs_realtime_pb2
import google.protobuf

## 2. Bus System Analysis


#- Fetch Real time Lisbon Carris Trasnport
Carris_CL = None

def get_all_vehicle_data():
    global Carris_CL
    url = "https://gateway.carris.pt/gateway/gtfs/api/v2.11/GTFS/realtime/vehiclepositions"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to get data: {response.status_code}")
        Carris_CL = None
        return

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    data_list = []

    def parse_message(message, prefix=''):
        result = {}
        for field, value in message.ListFields():
            name = f"{prefix}{field.name}"
            if hasattr(value, 'ListFields'):
                result.update(parse_message(value, prefix=name+'_'))
            else:
                result[name] = value
        return result

    for entity in feed.entity:
        if entity.HasField('vehicle'):
            vehicle_data = parse_message(entity.vehicle)
            vehicle_data['entity_id'] = entity.id
            data_list.append(vehicle_data)

    if data_list:
        Carris_CL = pd.DataFrame(data_list)
        
        # Mapeamento dos status
        status_map = {
            0: 'INCOMING_AT',
            1: 'STOPPED_AT',
            2: 'IN_TRANSIT_TO'
        }
        
     
        if 'current_status' in Carris_CL.columns:
            Carris_CL['current_status_text'] = Carris_CL['current_status'].map(status_map)
        else:
            print("Coluna 'current_status' não encontrada no DataFrame.")
        
        print(f"Got {len(Carris_CL)} vehicles with {len(Carris_CL.columns)} columns.")
        print(tabulate(Carris_CL.head(), headers='keys', tablefmt='grid', showindex=False))
    else:
        print("No vehicle data found.")
        Carris_CL = None


# Call function to update Carris_CL
get_all_vehicle_data()

#- Prepare the Dataset for combine

# Convert timestamp to datetime
Carris_CL['datetime'] = pd.to_datetime(Carris_CL['timestamp'], unit='s')

Carris_CL['Date'] = Carris_CL['datetime'].dt.date
Carris_CL['Time'] = Carris_CL['datetime'].dt.time


# drop unnecessary columns
drop_columns = ['current_stop_sequence', 'current_status', 'trip_schedule_relationship',
                'current_stop_sequence', 'trip_id',
                'vehicle_id', ' vehicle_license_plate','timestamp','datetime']

drop_columns = [col.strip() for col in drop_columns]

Carris_CL.drop(columns=drop_columns, inplace=True, errors='ignore')

# rename columns
Carris_CL.rename(columns={
    'entity_id': 'Line_ID',
    'position_latitude': 'Latitude',
    'position_longitude': 'Longitude',
    'current_status_text': 'Current_Status',
    'trip_route_id': 'Route_ID',
    'stop_id': 'Stop_ID',
    'trip_trip_id': 'Trip_ID',
    'trip_direction_id': 'Direction_ID'
}, inplace=True)

# Replace values in the 'Current_Status' column
Carris_CL['Current_Status'] = Carris_CL['Current_Status'].replace({
    'IN_TRANSIT_TO': 'In Transit',
    'INCOMING_AT': 'Approaching',
    'STOPPED_AT': 'Stationed'
})

# Keep last 3 characters of Line_ID
Carris_CL['Line_ID'] = Carris_CL['Line_ID'].str[-3:]


print(tabulate(Carris_CL.head(), headers='keys', tablefmt='grid', showindex=False))


#- Fetch Real time Lisbon Carris Metropolitana Transport


Carris_CM = None  # global variable

def get_realtime_bus_positions():
    global Carris_CM

    url = "https://api.carrismetropolitana.pt/v1/vehicles"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if data:
            Carris_CM = pd.DataFrame(data)
            print(f"\nFound {len(Carris_CM)} vehicles. Showing first 5:")
            print(tabulate(Carris_CM.head(), headers='keys', tablefmt='grid', showindex=False))
        else:
            print("No vehicles found.")
            Carris_CM = None
            
    except requests.RequestException as e:
        print("Error fetching data:", e)
        Carris_CM = None

# Call the function to update Carris_CM
get_realtime_bus_positions()


### 2.1 Data Processing and Column Renaming
#Process the raw vehicle data and rename columns for better readability. This step makes the data more intuitive to work with by:
#- Renaming columns with more descriptive names
#- Standardizing column naming conventions

# Convert vehicle_data to a DataFrame
Carris_CM = pd.DataFrame(Carris_CM)

# Rename the columns for better readability
def rename_columns(Carris_CM):
    Carris_CM.rename(columns={
        'id': 'Vehicle ID',
        'line_id': 'Line_ID',
        'lat': 'Latitude',
        'lon': 'Longitude',
        'route_id': 'Route_ID',
        'current_status': 'Current_Status',
        'bearing': 'Direction_ID',
        'pattern_id': 'Pattern_ID',
        'speed': 'Speed',
        'timestamp': 'Timestamp',
        'shift_id': 'Shift_ID',
        'stop_id': 'Stop_ID',
        'trip_id': 'Trip_ID',
        'block_id': 'Travel_ID',
    }, inplace=True)
    return Carris_CM

# Rename columns
Carris_CM = rename_columns(Carris_CM)


# Drop Vehicle ID column
#Carris_CM.drop(columns=['Vehicle ID'], inplace=True)

# Replace values in the 'Current_Status' column
Carris_CM['Current_Status'] = Carris_CM['Current_Status'].replace({
    'IN_TRANSIT_TO': 'In Transit',
    'INCOMING_AT': 'Approaching',
    'STOPPED_AT': 'Stationed'
})


print(f"\nFound {len(Carris_CM)} vehicles. Showing first 5:")
print(tabulate(Carris_CM.head(), headers='keys', tablefmt='grid', showindex=False))
#- Combine the dataset 
# 1. Garantir que Carris_CL tenha somente as colunas de Carris_CM
Carris_CL_aligned = Carris_CL.reindex(columns=Carris_CM.columns)

# 2. Concatenar os dois DataFrames
Bus_System = pd.concat([Carris_CM, Carris_CL_aligned], ignore_index=True)

from tabulate import tabulate

print(f"Carris_CM shape: {Carris_CM.shape}")
print(f"Carris_CL shape (after alignment): {Carris_CL_aligned.shape}")
print(f"Bus_System shape: {Bus_System.shape}")

print("\nPreview:")
print(tabulate(Bus_System.head(), headers='keys', tablefmt='grid'))


# Save the merged DataFrame to a CSV file
Bus_System.to_csv('Bus_System.csv', index=False)
#- Load the CSV Color_ID
# Load the CSV Color_ID

color_df = pd.read_csv(r"D:\Project\Lisbon’s Public Transport Network\Datasets\Line_Colors.csv")

color_df.columns = color_df.columns.str.strip()

# 2. Renomear a coluna de chave para 'Line_ID' para facilitar o merge
color_df.rename(columns={'LineID': 'Line_ID'}, inplace=True)

Bus_System = Bus_System.merge(
    color_df[['Line_ID', 'Color_ID', 'Operator']],  # agora o nome está sem espaços
    on='Line_ID',
    how='left'
)
from tabulate import tabulate

print(tabulate(Bus_System.head(), headers='keys', tablefmt='grid', showindex=False, floatfmt=".6f"))



### 2.2 Timestamp Processing
#Convert Unix timestamps to human-readable datetime format:
#- Process real-time timestamps from vehicle data
#- Add formatted datetime information
#- Split into date and time components
# Verifica se a coluna 'Timestamp' existe
if 'Timestamp' in Bus_System.columns:
    # Converte timestamp para datetime e extrai componentes
    Bus_System.loc[:, 'Datetime'] = pd.to_datetime(Bus_System['Timestamp'], unit='s')
    Bus_System.loc[:, 'Date'] = Bus_System['Datetime'].dt.strftime('%Y-%m-%d')
    Bus_System.loc[:, 'Time'] = Bus_System['Datetime'].dt.strftime('%H:%M:%S')
    Bus_System.loc[:, 'Day'] = Bus_System['Datetime'].dt.day_name()
    Bus_System.loc[:, 'Workday'] = Bus_System['Datetime'].dt.dayofweek.apply(lambda x: x < 5)

    # Mostra o horário atual
    print(f"\nCurrent time: {pd.Timestamp.now()}")

    # Exibe resultados
    print("\nProcessed datetime information:")
    print(tabulate(
        Bus_System[['Line_ID', 'Latitude', 'Longitude', 'Date', 'Time', 'Day', 'Workday']].head(),
        headers='keys',
        tablefmt='psql',
        showindex=False,
        floatfmt='.6f'
    ))
else:
    print("Error: 'Timestamp' column not found in Bus_System DataFrame.")
### 2.3 Geographic Enrichment
#Enrich the bus position data with district information using reverse geocoding. This process:
#- Uses the Nominatim geocoding service
#- Implements rate limiting to avoid API restrictions
#- Adds district names based on latitude and longitude
#- Handles potential geocoding errors gracefully
def get_district(latitude, longitude):
    try:
        if pd.isna(latitude) or pd.isna(longitude):
            return 'Invalid coordinates'

        # Ensure geolocator is defined
        global geolocator
        location = geolocator.reverse((latitude, longitude), exactly_one=True)
        if location and location.raw.get('address'):
            address = location.raw['address']
            district = (address.get('suburb') or
                        address.get('neighbourhood') or
                        address.get('city_district') or
                        address.get('town') or
                        'Unknown')
            return district
    except Exception as e:
        print(f"Geocoding error at ({latitude}, {longitude}): {e}")
    return 'Unknown'


Bus_System['District'] = Bus_System.apply(
    lambda row: get_district(row['Latitude'], row['Longitude']),
    axis=1
)


print(tabulate(Bus_System.head(), headers='keys', tablefmt='grid', showindex=False, floatfmt=".6f"))
### 2.4 Data Cleaning and Organization
#Clean and organize the final dataset by:
#- Removing any leading/trailing spaces from column names
#- Selecting the most relevant columns for analysis
#- Creating a focused view of the transportation data
# Strip any leading/trailing spaces from column names
Bus_System.columns = Bus_System.columns.str.strip()

# Ensure 'Color_ID' and 'Operator' columns exist, add them with default values if missing
if 'Color_ID' not in Bus_System.columns:
    Bus_System['Color_ID'] = 'Unknown'  # Default value for missing 'Color_ID'
if 'Operator' not in Bus_System.columns:
    Bus_System['Operator'] = 'Unknown'  # Default value for missing 'Operator'

# Select relevant columns for the final DataFrame (removed 'Timestamp' as it does not exist)
Bus_System = Bus_System[['Line_ID', 'Color_ID', 'Operator', 'Current_Status', 'Latitude', 'Longitude', 'Speed', 'Stop_ID', 'District', 'Travel_ID', 'Date', 'Time', 'Day', 'Workday']]

# Display the final DataFrame
print(tabulate(Bus_System.head(),
               headers='keys',
               tablefmt='grid',
               showindex=False,
               floatfmt='.6f'))
### 2.5 Select the Relevant and Drop the rest of Columns

# Drop the original Timestamp and Datetime columns if they exist
columns_to_drop = ['Timestamp', 'Datetime', 'Variant', 'Serial', 'Route_Code', 'Trip_Serial', 'Trip_Date', 'Vehicle_Code', 'Route', 'Original_ID', 'Start_Time', 'School_Code', 'Prefix',]
existing_columns_to_drop = [col for col in columns_to_drop if col in Bus_System.columns]

Bus_System = Bus_System.drop(existing_columns_to_drop, axis=1)

print(tabulate(Bus_System.head(), headers='keys', tablefmt='psql'))
### 2.6 Character Encoding Management
import pandas as pd

# Example input list
places = [
    "Parque das NaÃ§Ãµes",
    "SÃ£o SebastiÃ£o",
    "Unknown"
]

# Fix encoding
def fix_encoding(text):
    try:
        return text.encode('latin1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text  # return original if it fails

# Apply to list (or DataFrame column)
fixed_places = [fix_encoding(place) for place in places]

# Show result

print(tabulate(Bus_System.head(), headers='keys', tablefmt='psql'))
#save to CSV
Bus_System.to_csv('bus_system_data.csv', index=False)
### 2.7 Data Type Configuration

from datetime import time  # Import datetime.time explicitly

# Define a dictionary mapping column names to their desired data types
dtype_mapping = {
    'Line_ID': 'string',  
    'Stop_ID': 'string', 
    'Current_Status': 'category',
    'District': 'category',
    'Day': 'category',
    'Latitude': 'float64',
    'Longitude': 'float64',
    'Speed': 'float64',
    'Date': 'datetime64[ns]',
    'Travel_ID': 'string',
    'Workday': 'bool'
}

# Apply the data type conversions
Bus_System = Bus_System.astype(dtype_mapping)

# Specifically handle the 'Time' column to ensure it remains in time format
if not isinstance(Bus_System['Time'].iloc[0], time):
    Bus_System['Time'] = pd.to_datetime(Bus_System['Time']).dt.time

# Display the updated data types
print("Updated DataFrame Data Types:")
print(tabulate(Bus_System.dtypes.reset_index(), headers=["Column", "Data Type"], tablefmt="grid"))


## 3 Database Integration in SQL
# Safely convert columns to the desired data types
Bus_System['Line_ID'] = pd.to_numeric(Bus_System['Line_ID'], errors='coerce').fillna(0).astype(int)
Bus_System['Current_Status'] = Bus_System['Current_Status'].astype(str)
Bus_System['Latitude'] = pd.to_numeric(Bus_System['Latitude'], errors='coerce').fillna(0.0).astype(float)
Bus_System['Longitude'] = pd.to_numeric(Bus_System['Longitude'], errors='coerce').fillna(0.0).astype(float)
Bus_System['Speed'] = pd.to_numeric(Bus_System['Speed'], errors='coerce').fillna(0.0).astype(float)
Bus_System['Stop_ID'] = pd.to_numeric(Bus_System['Stop_ID'], errors='coerce').fillna(0).astype(int)
Bus_System['District'] = Bus_System['District'].astype(str)
Bus_System['Travel_ID'] = Bus_System['Travel_ID'].astype(str)
Bus_System['Date'] = pd.to_datetime(Bus_System['Date'], errors='coerce')
Bus_System['Time'] = pd.to_datetime(Bus_System['Time'], errors='coerce').dt.time
Bus_System['Day'] = pd.to_numeric(Bus_System['Day'], errors='coerce').fillna(0).astype(int)
Bus_System['Workday'] = pd.to_numeric(Bus_System['Workday'], errors='coerce').fillna(0).astype(int)

print("Updated DataFrame Data Types:")
print(tabulate(Bus_System.dtypes.reset_index(), headers=["Column", "Data Type"], tablefmt="grid"))
### 3.1 Database Connection
#- Visualizer the drive
# Example DataFrame for demonstration
pyodbc.drivers()
### 3.2 Connect to Database on SQL Server
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=STATION-DEV\\SQLEXPRESS08;'  # Double backslash to escape it
    'DATABASE=Bus_System;'
    'Trusted_Connection=yes;'
)

# If connection is successful, you can do a simple query
print("Connection successful!")
### 3.3 Insert Data into SQL Table
# Ensure Travel_ID is a string
Bus_System['Travel_ID'] = Bus_System['Travel_ID'].astype(str)

# Create a cursor object
cursor = conn.cursor()

# Insert data into the Bus_Status table
for index, row in Bus_System.iterrows():
    cursor.execute('''
        INSERT INTO Bus_Status (
            Line_ID, Current_Status, Latitude, Longitude, Speed, 
            Stop_ID, District, Travel_ID, Date, Time, Day, Workday
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', row['Line_ID'], row['Current_Status'], row['Latitude'], row['Longitude'], row['Speed'], 
       row['Stop_ID'], row['District'], row['Travel_ID'], row['Date'], row['Time'], row['Day'], row['Workday'])

# Commit the transaction
conn.commit()

# Close the cursor
cursor.close()

# If connection is successful, you can do a simple query
print("Input Data successful!")
