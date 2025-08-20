import pandas as pd
import numpy as np
from faker import Faker
from sqlalchemy import create_engine
import random
from datetime import datetime, timedelta

# --- CONFIGURATION ---
NUM_DEPOTS = 10
NUM_TRUCKS = 50
NUM_SHIPMENTS = 300
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 8, 8)

# Initialize Faker for Indian context
fake = Faker('en_IN')

# --- DATABASE CONNECTION DETAILS ---
# This should be the password you created in MySQL
db_user = 'project_user'
db_password = 'Abinbev%402025'
db_host = 'localhost'
db_name = 'abinbev_india_logistics'

# --- 1. GENERATE DATA ---

# Depots Data
print("Generating depots data...")
depots_data = []
indian_cities = ['Delhi', 'Mumbai', 'Bangalore', 'Chennai', 'Kolkata', 'Hyderabad', 'Pune', 'Ahmedabad', 'Jaipur', 'Lucknow']
for i in range(NUM_DEPOTS):
    city = indian_cities[i]
    depots_data.append({
        'DepotID': f'DEP{i+1:03}',
        'DepotName': f'{city} Central Depot',
        'DepotCity': city,
        'State': fake.state()
    })
depots_df = pd.DataFrame(depots_data)

# Trucks Data
print("Generating trucks data...")
trucks_data = []
for i in range(NUM_TRUCKS):
    trucks_data.append({
        'TruckID': f'TRK{i+1:03}',
        'TruckNumber': f'{random.choice(["HR", "DL", "MH", "KA"])}-{random.randint(10,99)}-{chr(random.randint(65,90))}{chr(random.randint(65,90))}-{random.randint(1000,9999)}',
        'TruckType': random.choices(['Refrigerated', 'Non-Refrigerated'], weights=[0.7, 0.3], k=1)[0],
        'TruckAge_Years': random.randint(0, 10)
    })
trucks_df = pd.DataFrame(trucks_data)

# Shipments Data
print("Generating shipments data...")
shipments_data = []
for i in range(NUM_SHIPMENTS):
    dispatch_time = fake.date_time_between(start_date=START_DATE, end_date=END_DATE)
    duration_hours = random.uniform(10, 72)
    arrival_time = dispatch_time + timedelta(hours=duration_hours)
    shipments_data.append({
        'ShipmentID': f'SHP{i+1:05}',
        'DepotID': f'DEP{random.randint(1, NUM_DEPOTS):03}',
        'TruckID': f'TRK{random.randint(1, NUM_TRUCKS):03}',
        'DispatchTime': dispatch_time,
        'ArrivalTime': arrival_time,
        'RouteName': f'Haryana Brewery - {random.choice(depots_df["DepotCity"])}'
    })
shipments_df = pd.DataFrame(shipments_data)

# Sensor Readings Data
print("Generating sensor readings data (this is the longest step)...")
sensor_readings_data = []
reading_id_counter = 1
for _, shipment in shipments_df.iterrows():
    truck_type = trucks_df[trucks_df['TruckID'] == shipment['TruckID']]['TruckType'].iloc[0]
    current_time = shipment['DispatchTime']

    # Simulate a 5% chance of cooler failure for refrigerated trucks
    cooler_fails = (truck_type == 'Refrigerated' and random.random() < 0.05)
    failure_time = shipment['DispatchTime'] + timedelta(hours=random.uniform(2, (shipment['ArrivalTime'] - shipment['DispatchTime']).total_seconds() / 3600 - 1)) if cooler_fails else None

    while current_time < shipment['ArrivalTime']:
        base_temp = 4.0

        # Simulate temperature for non-refrigerated trucks based on time of day
        if truck_type == 'Non-Refrigerated':
            if 6 <= current_time.hour <= 18: # Daytime
                base_temp = 25.0 + 10 * np.sin((current_time.hour - 6) * np.pi / 12)
            else: # Nighttime
                base_temp = 20.0
        # Simulate temperature for a failed refrigerated truck
        elif cooler_fails and current_time > failure_time:
            hours_since_failure = (current_time - failure_time).total_seconds() / 3600
            base_temp = 4.0 + hours_since_failure * 5

        # Add random noise to the temperature
        temperature = round(base_temp + random.uniform(-0.5, 0.5), 2)
        temperature = min(temperature, 45.0) # Cap temperature at 45 C

        sensor_readings_data.append({
            'ReadingID': reading_id_counter,
            'ShipmentID': shipment['ShipmentID'],
            'Timestamp': current_time,
            'Temperature_Celsius': temperature
        })
        reading_id_counter += 1
        current_time += timedelta(minutes=30)

sensor_readings_df = pd.DataFrame(sensor_readings_data)


# --- 2. LOAD DATA INTO MYSQL ---
try:
    # Create a connection engine to the MySQL database
    engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}')
    print("\nConnecting to MySQL database...")

    # Load each DataFrame into its corresponding SQL table
    depots_df.to_sql('depots', con=engine, if_exists='replace', index=False)
    print("-> Successfully loaded 'depots' table.")
    trucks_df.to_sql('trucks', con=engine, if_exists='replace', index=False)
    print("-> Successfully loaded 'trucks' table.")
    shipments_df.to_sql('shipments', con=engine, if_exists='replace', index=False)
    print("-> Successfully loaded 'shipments' table.")
    sensor_readings_df.to_sql('sensor_readings', con=engine, if_exists='replace', index=False)
    print("-> Successfully loaded 'sensor_readings' table.")

    print("\n✅ Phase 1 Complete! Your database is ready.")

except Exception as e:
    print(f"\n❌ An error occurred: {e}")