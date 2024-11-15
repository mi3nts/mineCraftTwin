
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



nodeIDReal    =  ["2cf7f12032305202","2cf7f12032304ba6","2cf7f12032304a10","2cf7f120323075e4","2cf7f12032303bdd","2cf7f12032304e7d","2cf7f120323020b9"]

nodeIDMock   =  ["2cf7f12032304b53","2cf7f12032303e79","2cf7f12032304ce7","2cf7f12032304a0d","2cf7f12032303476"] 

latLongMock   =  [[32.98597938739401, -96.74794966730421],\
                  [32.984865666666664, -96.74789583333335],\
                  [32.98500116666666, -96.75052533333334],\
                  [32.991720666666666, -96.75582883333333],\
                  [32.98583516666667, -96.75260566666665]]

# Function to get a nodeID from the combined list, with index wrapping
def get_nodeID(index):
    # Use modulus to wrap index if it exceeds the list length
    return nodeIDReal[index % len(nodeIDReal)]


for index,nodeID in enumerate(nodeIDMock):
    
    realIDForMock            = get_nodeID(index)
    merged_df = pd.read_pickle(f"{realIDForMock}_all_sensors.pkl")
    
    print(nodeID)
    if merged_df is not None:
        print(merged_df.head())
        merged_df["latitudeCoordinate"]  =  latLongMock[index][0]
        merged_df["longitudeCoordinate"] =  latLongMock[index][1]
        print(merged_df.head())

        # Save to a pickle file
        output_file_pkl = f"{nodeID}_all_sensors.pkl"
        output_file_csv = f"{nodeID}_all_sensors.csv"
        
        merged_df.to_pickle(output_file_pkl)
        merged_df.to_csv(output_file_csv)

        print(f"Saved {output_file_pkl} with merged data for Node {nodeID}.")
    else:
        print(f"No data available for Node {nodeID}.")
