import paho.mqtt.client as mqttclient
import time
import json
import pandas as pd
import datetime

BROKER_ADDRESS = "localhost"
PORT = 1883
THINGS_BOARD_ACCESS_TOKEN = "0ofrmsx5ROxFWLgxyb8l"


def subscribed(client, userdata, mid, granted_qos):
    print("Subscribed...")


def recv_message(client, userdata, message):
    print("Received: ", message.payload.decode("utf-8"))
    temp_data = {'value': True}
    try:
        jsonobj = json.loads(message.payload)
        if jsonobj['method'] == "setValue":
            temp_data['value'] = jsonobj['params']
            client.publish('v1/devices/me/attributes', json.dumps(temp_data), 1)
    except:
        pass


def connected(client, usedata, flags, rc):
    if rc == 0:
        print("Thingsboard connected successfully!!")
        client.subscribe("v1/devices/me/rpc/request/+")
    else:
        print("Connection is failed")


client = mqttclient.Client("Flood 1")
client.username_pw_set(THINGS_BOARD_ACCESS_TOKEN)

client.on_connect = connected
client.connect(BROKER_ADDRESS, PORT)
client.loop_start()

client.on_subscribe = subscribed
client.on_message = recv_message

# Read Data from csv.file
data = pd.read_csv('Data/Sensor_3.csv')

rainfall = data.iloc[:, 1]
tide = data.iloc[:, 2]
flooded = data.iloc[:, 3]
counter = 1

while True:
    if counter == len(data):
        break
    collect_data = {'id': 3, 'latitude': 10.8018862, 'longitude': 106.7227111, 'rainfall': rainfall[counter],
                    'tide': tide[counter], 'flooded': flooded[counter], 'time': str(datetime.datetime.now())}
    client.publish('v1/devices/me/telemetry', json.dumps(collect_data), 1)
    counter += 1
    time.sleep(30)
