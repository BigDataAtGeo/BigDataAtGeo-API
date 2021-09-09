import os
import util
import requests
from typing import Dict, Union, List, Tuple
from geojson import FeatureCollection
from flask import Flask, Response, abort, jsonify, request, escape, send_file
from flask_caching import Cache
from flask_compress import Compress
from flask_cors import CORS
from rocketchat.api import RocketChatAPI
from validate_email import validate_email

path = os.environ["BDATG_REST_API_PATH"]
public_key = os.environ["FIELD_CLIMATE_PUBLIC_KEY"]
private_key = os.environ["FIELD_CLIMATE_PRIVATE_KEY"]
fieldclimate_api = "https://api.fieldclimate.com/v1"

username = os.environ["CHAT_USERNAME"]
password = os.environ["CHAT_PASSWORD"]

config = {
    "DEBUG": False,
    "CACHE_TYPE": "simple",
    "CACHE_DEFAULT_TIMEOUT": 60 * 60 * 24
}

app = Flask(__name__)
app.config.from_mapping(config)
cache = Cache(app)
Compress(app)
CORS(app)

app.json_encoder = util.UJSONEncoder

data = util.load_data(path)
scenarios, variables, timeranges, stations = util.load_meta(data)

api = RocketChatAPI(settings={'username': username, 'password': password,
                              'domain': 'https://chat.informatik.uni-wuerzburg.de'})

print("ready")


@app.route("/fieldclimate/sources")
@cache.cached()
def fieldclimate_sources() -> Union[Response, Tuple]:
    api_route = "/user/stations"
    auth = util.AuthHmacMetosGet(api_route, public_key, private_key)
    response = requests.get(fieldclimate_api + api_route, headers={'Accept': 'application/json'}, auth=auth).json()

    sources = []
    for station in response:
        sources.append({
            "name": station["name"],
            "position": station["position"],
            "dates": station["dates"]
        })
        station_id = station["name"]["original"]
        if station_id in stations and "text" in stations[station_id]:
            sources[-1]["text"] = stations[station_id]["text"]

    return jsonify(sources)


@app.route("/fieldclimate/data/<station>/<group>/<from_ts>/<to_ts>")
@cache.cached()
def fieldclimate_data(station, group, from_ts, to_ts) -> Union[Response, Tuple]:
    api_route = f"/data/optimized/{station}/{group}/from/{from_ts}/to/{to_ts}"
    auth = util.AuthHmacMetosGet(api_route, public_key, private_key)
    return requests.get(fieldclimate_api + api_route, headers={'Accept': 'application/json'}, auth=auth).json()


@app.route("/all_locations/grid/<scenario>/<variable>/<timerange>", methods=["GET"])
@cache.cached()
def all_locations_grid(scenario: str, variable: str, timerange: str) -> Union[Response, FeatureCollection]:
    rows = data[(data["variable"] == variable) & (data["scenario"] == scenario) & (data["timerange"] == timerange)]
    features = rows.apply(util.point_to_feature, axis=1).tolist()

    return FeatureCollection(features)


@app.route("/all_locations/values/<scenario>/<var>/<timerange>", methods=["GET"])
@cache.cached()
def all_locations_values(scenario: str, var: str, timerange: str) -> Union[Response, Dict]:
    rows = data[(data["variable"] == var) & (data["scenario"] == scenario) & (data["timerange"] == timerange)]

    return dict(zip(rows.id, rows.value))


@app.route("/all_times/<int:cell_id>/<scenario>/<var>", methods=["GET"])
@cache.cached()
def all_times(cell_id: int, scenario: str, var: str) -> Union[Response, Dict]:
    rows = data[(data["variable"] == var) & (data["scenario"] == scenario) & (data["id"] == cell_id)]

    if rows.shape[0] == 0:
        abort(404)

    return {
        "lat": rows.iloc[0].lat,
        "lon": rows.iloc[0].lon,
        "data": {
            "keys": rows.timerange.tolist(),
            "values": rows.value.tolist()
        }
    }


@app.route("/index", methods=["GET"])
@cache.cached()
def index() -> Union[Response, Dict]:
    return {
        "variables": variables,
        "scenarios": scenarios,
        "timeranges": timeranges
    }

@app.route('/feedback', methods = ['POST'])
def feedback():
   data = request.get_json()

   name = escape(data['name'])
   email = escape(data['email'])
   feedback = data['feedback']

   is_valid =  validate_email(email_address=email, check_format=True, check_smtp=False)
   if is_valid or email == "":
        message = "Name: " + name + "\n" +"Email: " + email + "\n" + "Feedback: " + feedback
        # rytayLo3qcooGD8sd is the 'bigdata-at-geo-feedback' Room-ID
        api.send_message(message, 'rytayLo3qcooGD8sd') 
        return jsonify(data) 

   return Response(status=400, response="Email is not valid") 


@app.route("/images/<station>/<index>")
def station_images(station, index) -> Union[Response, Tuple]:
    path = "data/images/" + station + "/" + index + ".png"
    return send_file(path, mimetype='image/png')   


@app.route("/information/<station>")
def get_info(station):
    path = "data/info/" + station + ".json"  
    return send_file(path, mimetype="application/json")
