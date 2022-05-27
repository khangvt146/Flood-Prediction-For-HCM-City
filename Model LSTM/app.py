import json
from flask import Flask, jsonify
from flask_cors import CORS, cross_origin
from flask import request
import numpy as np
from keras.models import load_model
from sklearn.preprocessing import MinMaxScaler

# Init Flask Server Backend
app = Flask(__name__)

# Apply Flask CORS
CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route('/', methods=['POST', 'GET'])
@cross_origin(origin='*')
def home_process():
    return 'Flood forecast'


@app.route('/predict', methods=['POST'])
@cross_origin(origin='*')
def predict_process():
    rainfall = request.json['rainfall']
    tide = request.json['tide']
    flooded = request.json['flooded']

    current = np.array([rainfall, tide, flooded]).T
    scaler = MinMaxScaler(feature_range=(0, 1))
    current = scaler.fit_transform(current)
    current = np.expand_dims(current, axis=0)

    model = load_model('./model.h5')
    predict = model.predict(current)
    predict = scaler.inverse_transform(predict)

    rainfall_pred = predict[0][0] if predict[0][0] > 0 else 0
    tide_pred = predict[0][1] if predict[0][1] else 0
    flooded_pred = predict[0][2] if predict[0][2] else 0

    return jsonify({'rainfall': float(rainfall_pred), 'tide': float(tide_pred), 'flooded': float(flooded_pred)})

# Start Backend
if __name__ == '__main__':
    app.run(host='localhost', port=4000, debug=True)
