from confluent_kafka import DeserializingConsumer
import paho.mqtt.client as mqttclient
import json
import requests
import copy
import datetime

# KAFKA INIT
BOOTSTRAP_SEVER = "localhost:9092"
SCHEMA_REGISTRY = "http://localhost:8081"
TOPIC = "Flood_Sensor_3"
NUM_SENSOR = 1

# THINGSBOARD INIT
BROKER_ADDRESS = "localhost"
PORT = 1883
THINGS_BOARD_ACCESS_TOKEN = "I1cLkPS1GaO0puUh5rge"

DATA_QUEUE = {'rainfall': [], 'tide': [], 'flooded': []}
TIME_FUTURE = 1


def subscribed(client, userdata, mid, granted_qos):
    print("Subscribed...")


def connected(client, usedata, flags, rc):
    if rc == 0:
        print("Thingsboard connected successfully!!")
        client.subscribe("v1/devices/me/rpc/request/+")
    else:
        print("Connection is failed")


def make_predict(QUEUE):
    counter = TIME_FUTURE
    while counter > 0:
        predict_data = call_api(QUEUE)
        # Remove fist element
        QUEUE['rainfall'].pop(0)
        QUEUE['tide'].pop(0)
        QUEUE['flooded'].pop(0)

        # Add new element at end
        QUEUE['rainfall'] += [predict_data['rainfall']]
        QUEUE['tide'] += [predict_data['tide']]
        QUEUE['flooded'] += [predict_data['flooded']]
        counter -= 0.5

    return QUEUE['flooded'][-1]


def flood_level(flooded):
    if flooded > 0 and flooded < 15000:
        return "low"
    elif flooded >= 15000 and flooded < 30000:
        return "medium"
    elif flooded >= 30000:
        return "high"


client = mqttclient.Client("Sensor 2")
client.username_pw_set(THINGS_BOARD_ACCESS_TOKEN)

client.on_connect = connected
client.connect(BROKER_ADDRESS, 1883)
client.loop_start()

client.on_subscribe = subscribed


def main():
    consumer_conf = {'bootstrap.servers': BOOTSTRAP_SEVER,
                     'group.id': "1",
                     'auto.offset.reset': "earliest"}

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([TOPIC])
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            user = msg.value()
            user = json.loads(user.decode('utf-8'))
            if user is not None:
                if len(DATA_QUEUE['rainfall']) < 10:
                    DATA_QUEUE['rainfall'] += [user['rainfall']]
                    DATA_QUEUE['tide'] += [user['tide']]
                    DATA_QUEUE['flooded'] += [user['flooded']]
                    print(DATA_QUEUE)
                    continue
                else:
                    predict = make_predict(copy.deepcopy(DATA_QUEUE))
                    # Remove fist element
                    DATA_QUEUE['rainfall'].pop(0)
                    DATA_QUEUE['tide'].pop(0)
                    DATA_QUEUE['flooded'].pop(0)

                    # Add new element at end
                    DATA_QUEUE['rainfall'] += [user['rainfall']]
                    DATA_QUEUE['tide'] += [user['tide']]
                    DATA_QUEUE['flooded'] += [user['flooded']]

                    level = flood_level(predict)

                    predict_data = {'flooded': predict,
                                    'time': str(datetime.datetime.now() + datetime.timedelta(hours=2)), 'level': level}
                    print(predict_data)
                    client.publish('v1/devices/me/telemetry', json.dumps(predict_data), 1)

        except KeyboardInterrupt:
            break

    consumer.close()


def call_api(collect_data):
    url = "http://localhost:4000/predict"
    payload = json.dumps(collect_data)
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()


if __name__ == '__main__':
    main()
