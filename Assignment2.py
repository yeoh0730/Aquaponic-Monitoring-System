import pandas as pd
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json

uri = "mongodb+srv://yeoh0730:<password>@cpc357.w0vmwin.mongodb.net/?retryWrites=true&w=majo>

# Create a new client and connect to the server
mongo_client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    mongo_client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

db = mongo_client["aquaponicfishpond"]
collection = db["fishpond"]

# MQTT configuration
mqtt_broker_address = "34.55.198.79"  # Replace with your VM instance external IP
mqtt_topic = "fishpond"

# Define the callback function for connection
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"Successfully connected")
        client.subscribe(mqtt_topic)
        
        # Move the data publishing inside the on_connect
        excel_data = pd.read_csv('/home/yeohhueyjing111/pond_iot_2024.csv')  # Adjust your path
        for index, row in excel_data.iterrows():
            message = row.to_json()
            print("Publishing: ", message)  # Debug print
            client.publish(mqtt_topic, message)

# Define the callback function for ingesting data into MongoDB
def on_message(client, userdata, message):
    payload = message.payload.decode("utf-8")
    print(f"Received message: {payload}")
    
    try:
        # Parse the payload JSON string into a dictionary
        parsed_data = json.loads(payload)

        # Add a timestamp field
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        parsed_data["timestamp"] = timestamp

        # Insert parsed data directly into MongoDB
        collection.insert_one(parsed_data)
        print("Data ingested into MongoDB")
    except Exception as e:
        print(f"Error processing or inserting data: {e}")

# Create a MQTT client instance
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

# Attach the callbacks using explicit methods
client.on_connect = on_connect
client.on_message = on_message

# Connect to MQTT broker
client.connect(mqtt_broker_address, 1883, 60)

# Start the MQTT loop
client.loop_forever()
