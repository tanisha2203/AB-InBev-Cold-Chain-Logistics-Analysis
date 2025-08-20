import pandas as pd
from sqlalchemy import create_engine
from datetime import timedelta

# --- 1. CONFIGURATION & CONNECTION ---
# Database connection details (ensure password is correct)
db_user = 'project_user'
db_password = 'Abinbev%402025' # Remember the password is URL encoded
db_host = 'localhost'
db_name = 'abinbev_india_logistics'

# Create the database connection engine
try:
    engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}')
    print("Successfully connected to the database.")
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit()

# --- 2. DATA EXTRACTION ---
# SQL query to join all tables and get a complete dataset
sql_query = """
SELECT
    s.ShipmentID,
    s.RouteName,
    s.DispatchTime,
    s.ArrivalTime,
    t.TruckID,
    t.TruckType,
    t.TruckAge_Years,
    d.DepotCity,
    d.State,
    sr.Timestamp,
    sr.Temperature_Celsius
FROM
    shipments s
LEFT JOIN
    trucks t ON s.TruckID = t.TruckID
LEFT JOIN
    depots d ON s.DepotID = d.DepotID
LEFT JOIN
    sensor_readings sr ON s.ShipmentID = sr.ShipmentID
ORDER BY
    s.ShipmentID, sr.Timestamp;
"""

print("Extracting data from database...")
# Load the data directly into a pandas DataFrame
df = pd.read_sql(sql_query, engine)
print(f"Data extraction complete. Found {len(df)} total sensor readings.")


# --- 3. IDENTIFY SPOILED SHIPMENTS ---
print("Analyzing shipments for spoilage...")

# Business Rule: Spoilage occurs if temp > 25°C for more than 4 consecutive hours.
spoilage_threshold_temp = 25.0
spoilage_threshold_duration = timedelta(hours=4)

spoiled_shipments = set()
# Group by each individual shipment to analyze them one by one
for shipment_id, group in df.groupby('ShipmentID'):
    # Sort by time to ensure correct order
    group = group.sort_values(by='Timestamp')
    
    time_above_threshold = timedelta()
    
    for i in range(1, len(group)):
        # Calculate time difference between consecutive readings
        time_diff = group['Timestamp'].iloc[i] - group['Timestamp'].iloc[i-1]
        
        # Check if the previous reading was above the temperature threshold
        if group['Temperature_Celsius'].iloc[i-1] > spoilage_threshold_temp:
            time_above_threshold += time_diff
        else:
            # If temp drops, reset the counter
            time_above_threshold = timedelta()
            
        # If the cumulative time above threshold is met, flag as spoiled and break
        if time_above_threshold >= spoilage_threshold_duration:
            spoiled_shipments.add(shipment_id)
            break

# Create the new 'IsSpoiled' column based on our findings
df['IsSpoiled'] = df['ShipmentID'].isin(spoiled_shipments)
print(f"Analysis complete. Found {len(spoiled_shipments)} spoiled shipments.")


# --- 4. ROOT CAUSE ANALYSIS ---
print("\n--- Root Cause Analysis ---")
# Create a DataFrame with unique shipments to avoid double counting
shipments_summary = df.drop_duplicates(subset='ShipmentID').copy()

# 4.1 Analysis by Truck Type
spoilage_by_truck_type = shipments_summary.groupby('TruckType')['IsSpoiled'].value_counts(normalize=True).unstack(fill_value=0)
spoilage_by_truck_type['SpoilageRate_%'] = spoilage_by_truck_type[True] * 100
print("\nSpoilage Rate by Truck Type:")
print(spoilage_by_truck_type[['SpoilageRate_%']].round(2))

# 4.2 Analysis by Route
spoilage_by_route = shipments_summary.groupby('RouteName')['IsSpoiled'].mean().reset_index()
spoilage_by_route.rename(columns={'IsSpoiled': 'SpoilageRate'}, inplace=True)
top_5_risky_routes = spoilage_by_route.sort_values(by='SpoilageRate', ascending=False).head(5)
top_5_risky_routes['SpoilageRate_%'] = top_5_risky_routes['SpoilageRate'] * 100
print("\nTop 5 Highest-Risk Routes:")
print(top_5_risky_routes[['RouteName', 'SpoilageRate_%']].round(2))


# --- 5. SAVE THE OUTPUT ---
output_filename = 'shipment_analysis_output.csv'
df.to_csv(output_filename, index=False)
print(f"\n✅ Phase 2 Complete! Full analysis results saved to '{output_filename}'")