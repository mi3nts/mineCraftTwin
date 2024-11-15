
import pandas as pd
import glob

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

nodeIDs = ["2cf7f12032305202","2cf7f12032304ba6","2cf7f12032304b53","2cf7f12032303e79","2cf7f12032304ce7","2cf7f12032304a10","2cf7f120323075e4","2cf7f12032303bdd","2cf7f12032304a0d","2cf7f12032304e7d","2cf7f12032303476","2cf7f120323020b9"]

complete = None

for nodeID in nodeIDs:

    print(nodeID)
    merged_df = pd.read_pickle(f"{nodeID}_all_sensors.pkl")
    # Get the current column names (ignoring the index)
    column_names = merged_df.columns

    # Prepend the nodeID to each column name, except the index
    new_column_names = [f"{nodeID}_{col}" for col in column_names]

    # Set the new column names
    merged_df.columns = new_column_names

    # Merge with the existing merged DataFrame
    if complete is None:
        complete = merged_df
    else:
        complete = pd.merge(
            complete,
            merged_df,
            left_index=True,
            right_index=True,
            how='outer'
            )

# Fill gaps in the merged DataFrame
if complete is not None:
    complete.fillna(method='ffill', inplace=True)  # Forward fill missing values
    complete.fillna(method='bfill', inplace=True)  # Backward fill remaining missing values
    
    print(complete.head())
    # Save to a pickle file
    output_file_pkl = "complete_data_set.pkl"
    output_file_csv = "complete_data_set.csv"
    
    complete.to_pickle(output_file_pkl)
    complete.to_csv(output_file_csv)
    print(f"Saved {complete} with merged data for Node {nodeID}.")



else:
    print(f"No data available for Node {nodeID}.")
