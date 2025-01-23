import pandas as pd
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
import pymongo

# MongoDB configuration
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo_client["aquaponicfishpond"]
collection = db["iot"]

# MQTT configuration
mqtt_broker_address = "34.55.198.79"  # Replace with your VM instance external IP
mqtt_topic = "iot"

# Define the callback function for connection
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"Successfully connected")
        client.subscribe(mqtt_topic)
        
        # Move the data publishing inside the on_connect
        excel_data = pd.read_csv('/home/yeohhueyjing111/pond_iot_2023.csv')  # Adjust your path accordingly
        for index, row in excel_data.iterrows():
            message = row.to_json()
            print("Publishing: ", message)  # Debug print
            client.publish(mqtt_topic, message)

# Define the callback function for ingesting data into MongoDB
def on_message(client, userdata, message):
    payload = message.payload.decode("utf-8")
    print(f"Received message: {payload}")
    
    # Convert MQTT timestamp to datetime
    timestamp = datetime.now(timezone.utc)
    datetime_obj = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    
    # Process the payload and insert into MongoDB with proper timestamp
    document = {"timestamp": datetime_obj, "data": payload}
    collection.insert_one(document)
    print("Data ingested into MongoDB")

# Create a MQTT client instance
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

# Attach the callbacks using explicit methods
client.on_connect = on_connect
client.on_message = on_message

# Connect to MQTT broker
client.connect(mqtt_broker_address, 1883, 60)

# Start the MQTT loop
client.loop_forever()
