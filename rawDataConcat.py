import pandas as pd
import glob
import os


# Node Name	Building 	
# POLO NODE 01	2cf7f12032305202 WSTC 	
# POLO NODE 03	2cf7f12032304ba6 PS1 	
# POLO NODE 05	2cf7f12032304b53 ATC	          Fake 
# POLO NODE 09	2cf7f12032303e79 JSOM - 2	      Fake 
# POLO NODE 17	2cf7f12032304ce7 AB	              Fake 
# POLO NODE 18	2cf7f12032304a10 JSOM - 1	      
# POLO NODE 20	2cf7f120323075e4 Univ. Dr sign	  
# POLO NODE 21	2cf7f12032303bdd ECSN	          
# POLO NODE 22	2cf7f12032304a0d PARKING LOT T 	  Fake 
# POLO NODE 23	2cf7f12032304e7d Jonsson Building 
# POLO NODE 24	2cf7f12032303476 PS4	          Fake 
# POLO NODE 25	2cf7f120323020b9 Soccer Shack     


# Define nodes and sensors
nodeIDs = [
    "2cf7f12032305202", \
        "2cf7f12032304ba6",\
            "2cf7f12032304a10", \
                "2cf7f120323075e4",\
                    "2cf7f12032303bdd",\
                        "2cf7f12032304e7d",\
                            "2cf7f120323020b9"
]

sensorIDs = ["IPS7100CNR", "BME688CNR", "GPGGAPL"]

# Base directory for raw data
base_dir = '/Users/lakitha/mintsData/rawMqtt'

# Loop through each node
for nodeID in set(nodeIDs):  # Use `set()` to ensure unique nodeIDs
    merged_df = None  # Initialize an empty DataFrame for merging

    for sensorID in sensorIDs:
        # Build file pattern for each sensor
        file_pattern = os.path.join(base_dir, f"*/*/*/*/MINTS_{nodeID}_{sensorID}_*.csv")
        csv_files = sorted(glob.glob(file_pattern))

        if not csv_files:
            print(f"No files found for Node {nodeID}, Sensor {sensorID}.")
            continue  # Skip to the next sensor if no files are found

        # Read and concatenate CSV files
        sensor_df = pd.concat((pd.read_csv(file) for file in csv_files), ignore_index=True)

        # Parse and filter datetime
        sensor_df['dateTime'] = pd.to_datetime(sensor_df['dateTime'], errors='coerce')
        sensor_df = sensor_df.dropna(subset=['dateTime'])
        sensor_df.set_index('dateTime', inplace=True)

        # Resample to 5-minute intervals and calculate the mean
        sensor_df_resampled = sensor_df.resample('5T').mean()

        # Merge with the existing merged DataFrame
        if merged_df is None:
            merged_df = sensor_df_resampled
        else:
            merged_df = pd.merge(
                merged_df,
                sensor_df_resampled,
                left_index=True,
                right_index=True,
                how='outer'
            )

    # Fill gaps in the merged DataFrame
    if merged_df is not None:
        merged_df.fillna(method='ffill', inplace=True)  # Forward fill missing values
        merged_df.fillna(method='bfill', inplace=True)  # Backward fill remaining missing values

        merged_df["latitudeCoordinate"]  =  merged_df["latitudeCoordinate"].mean()
        merged_df["longitudeCoordinate"] =  merged_df["longitudeCoordinate"].mean()


        # Set timezone to GMT (UTC)
        merged_df.index = merged_df.index.tz_localize('UTC')

        # Convert to CST (UTC-6)
        merged_df.index = merged_df.index.tz_convert('US/Central')


        cst_date = pd.Timestamp('2024-08-22')
        merged_df = merged_df[merged_df.index.date == cst_date.date()]
        
        # Save to a pickle file
        # Save to a pickle file
        output_file_pkl = f"{nodeID}_all_sensors.pkl"
        output_file_csv = f"{nodeID}_all_sensors.csv"
        
        merged_df.to_pickle(output_file_pkl)
        merged_df.to_csv(output_file_csv)
        print(f"Saved {output_file_pkl} with merged data for Node {nodeID}.")

    else:
        print(f"No data available for Node {nodeID}.")
