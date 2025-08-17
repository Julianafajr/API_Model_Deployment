import json
import os
import logging
import random
import time
import datetime
import threading
import pickle

from flask import Flask, jsonify, request
import pandas as pd
from paho.mqtt import client as mqtt_client
import mysql.connector

from firebase_admin import messaging
from firebase_admin import credentials
from firebase_admin import initialize_app

cred = credentials.Certificate('./secretKey.json')
default_app = initialize_app(cred)
registration_token = 'f-uAf6pCS3at-1Xm2kNHeS:APA91bE802BL72ciEKflDSUg35Y9QUUPoztfcfJcEiNA3oYJe8UZLuaZYtwoUSzo9k5Mo66q8gmDU5aPHJbtYF2HxVkhLoSaV0KkF7XzrHbk2gpyhTCSikY'

message = messaging.Message(
    data={
        'test': 'Notification'
        
    },
    notification=messaging.Notification(
        title='Warning',
        body='Kualitas Air Buruk',
    ),
    token=registration_token,
)

broker = 'broker.emqx.io'
port = 1883
temperatureTopic = "sensor/NTC/tempInC"
amoniaTopic = "sensor/MQ135/ammonia"
phTopic = "sensor/PH/phValue"
# Generate a Client ID with the subscribe prefix.
client_id = f'subscribe-{random.randint(0, 100)}'

data_memory = {
    "Suhu": 25,
    "Amonia": 0.2,
    "pH": 7
}

db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Test_Pass1",
    database="limonia"
)

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)

latest_data = pd.DataFrame()

try:
    cluster_model = pickle.load(open('cluster_model.pkl', 'rb'))
    rules = pickle.load(open('rules_model.pkl', 'rb'))
except Exception as e:
    logging.error(f"Error loading models: {e}")
    raise e

def insert_in_db(sensor_data, prediction_result):
    try:
        print('prediction result', prediction_result)
        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        if db_connection.is_connected():
            cursor = db_connection.cursor()
            cursor.execute(f"INSERT INTO device_data VALUES ({sensor_data["Suhu"]}, {sensor_data["Amonia"]}, {sensor_data["pH"]}, '{timestamp}')")
            if 1 in prediction_result['warnings']:
                cursor.execute(f"INSERT INTO notification VALUES ('Kualitas Air Buruk', '{timestamp}')")
            db_connection.commit()
            
                
    except Exception as e:
        logging.error(f"Error loading models: {e}")
        raise e

def predict_result(data_in):
    # Konversi data JSON ke DataFrame
    df = pd.DataFrame([data_in])
    df = apply_thresholds(df)
    # print(df)
    df = cluster(df, cluster_model)
    df['Binary Pattern'], df['Readable Pattern'] = zip(*df.apply(get_pattern, axis=1))
    
    # print(df)

    # Proses hasil prediksi
    pattern = []
    warning = []

    for i in range(len(df)):
        binary_pattern = df['Binary Pattern'][i]

        if '1' in binary_pattern:
            warning.append(1)

    # Kirimkan response
    response = {
        "data": df.to_dict(orient="records"),
        "warnings": warning,
    }
    return response

@app.route('/notifications', methods=['GET'])
def notifications():
    try:
        if db_connection.is_connected():
            cursor = db_connection.cursor(dictionary=True)
            cursor.execute("SELECT * FROM notification ORDER BY timestamp DESC LIMIT 10")
            result = cursor.fetchall()
        return jsonify(result)

    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

@app.route('/latest-device-data', methods=['GET'])
def get_latest_device_data():
    try:
        if db_connection.is_connected():
            cursor = db_connection.cursor()
            cursor.execute("SELECT * FROM device_data ORDER BY timestamp DESC LIMIT 1")
            result = cursor.fetchall()

        return jsonify(result)

    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return jsonify({"error": "Internal Server Error"}), 500


@app.route('/registration-token', methods=['POST'])
def registration_token():
    try:
        # Ambil data dari body request
        data = request.get_json()
        print(data)
        if not data:
            return jsonify({"error": "No data provided"}), 400

        return '', 201

    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return jsonify({"error": "Internal Server Error"}), 500


@app.route('/predict', methods=['POST'])
def predict():
    try:
        # Ambil data dari body request
        data = request.get_json()
        
        print(data)

        if not data:
            return jsonify({"error": "No data provided"}), 400

        return jsonify(predict_result(data))

    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

def apply_thresholds(df):
    column_thresholds = {
        'Suhu': (25.0, 30.0),
        'pH': (6.0, 9.0),
        'Amonia': (0.0, 0.8)
    }

    for column, thresholds in column_thresholds.items():
        lower_bound, upper_bound = thresholds

        # Convert the column to numeric values
        df[column] = pd.to_numeric(df[column], errors='coerce')

        # Apply thresholds
        df[f'{column.lower()}_thresholded'] = df[column].apply(lambda val: 1 if (val is not None) and (val > upper_bound or val < lower_bound) else 0)
    return df

def get_pattern(raw_features):
    pattern_binary = ''
    pattern_readable = ''

    for i in range(3):
        feature_name = raw_features.index[i]
        is_failure = raw_features.iloc[i + 3] == 1
        pattern_readable += f'{feature_name}: {"Failure" if is_failure else "Normal"}, '
        pattern_binary += str(int(raw_features.iloc[i + 3]))

    return pattern_binary, pattern_readable

def cluster(df, loaded_model):
    df['Condition'] = loaded_model.predict(df[['suhu_thresholded', 'ph_thresholded', 'amonia_thresholded']])
    return df

def send_mqtt_message(warning):
    print(f'{warning}')

######### MQTT Section ############

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc, properties):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port, 60)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        if msg.topic == temperatureTopic:
            data_memory["Suhu"] = float(msg.payload.decode())
        elif msg.topic == amoniaTopic:
            data_memory["Amonia"] = float(msg.payload.decode())
        elif msg.topic == phTopic:
            data_memory["pH"] = float(msg.payload.decode())
        result = predict_result(data_memory)
        warning = result["warnings"]
        if 1 in warning:
            response = messaging.send(message)
            print('Successfully sent message:', response)
        
        insert_in_db(data_memory, result)
    
    client.subscribe(temperatureTopic)
    client.subscribe(amoniaTopic)
    client.subscribe(phTopic)
    client.on_message = on_message


def setup_mqtt():
    print("setting up mqtt")
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()

def setup_db():
    if db_connection.is_connected():
        cursor = db_connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS device_data (suhu FLOAT, amonia FLOAT , ph FLOAT, timestamp TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS notification (message VARCHAR(255), timestamp TIMESTAMP)")

def setup_http_server():
    app.run(host="0.0.0.0", port=8080)

if __name__ == '__main__':
    setup_db()
    t1 = threading.Thread(target=setup_http_server)
    t2 = threading.Thread(target=setup_mqtt)
    t1.start()
    t2.start()