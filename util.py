import os
import glob
import math
import tqdm
import ujson
import requests
import pandas as pd
from datetime import datetime
from typing import Tuple, List
from flask.json import JSONEncoder
from geojson import Feature, Polygon
from Crypto.Hash import HMAC
from Crypto.Hash import SHA256
from requests.auth import AuthBase


class UJSONEncoder(JSONEncoder):
    """ Use ujson since serialization takes a lot of time and it's faster """
    def default(self, obj):
        try:
            return ujson.dumps(obj)
        except TypeError:
            return JSONEncoder.default(self, obj)


class AuthHmacMetosGet(AuthBase):
    # Creates HMAC authorization header for Metos REST service GET request.
    def __init__(self, apiRoute: str, publicKey: str, privateKey: str) -> None:
        self._publicKey = publicKey
        self._privateKey = privateKey
        self._method = 'GET'
        self._apiRoute = apiRoute

    def __call__(self, request: requests.Request) -> requests.Request:
        dateStamp = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        request.headers["Date"] = dateStamp
        msg = (self._method + self._apiRoute + dateStamp + self._publicKey).encode(encoding="utf-8")  # dateStamp
        h = HMAC.new(self._privateKey.encode(encoding="utf-8"), msg, SHA256)
        signature = h.hexdigest()
        request.headers["Authorization"] = "hmac " + self._publicKey + ':' + signature
        return request


def point_to_feature(row, raster_size_in_m: int = 1000) -> Feature:
    """
    This method creates square bounds around coordinates
    Refer to https://github.com/Leaflet/Leaflet/blob/32ea41baaa93adcefd34006a12ebe3b4ee7fda7d/src/geo/LatLng.js#L87
    :param row: pandas dataframe row
    :param raster_size_in_m: size of the square
    :return: GeoJSON Feature
    """
    lon, lat = row["lon"], row["lat"]
    lat_accuracy = 180 * raster_size_in_m / 40075017
    lng_accuracy = lat_accuracy / math.cos((math.pi / 180) * lat)
    sw = lon - lng_accuracy, lat - lat_accuracy
    se = lon - lng_accuracy, lat + lat_accuracy
    ne = lon + lng_accuracy, lat + lat_accuracy
    nw = lon + lng_accuracy, lat - lat_accuracy
    # five coordinates to close polygon, yes those brackets are necessary
    polygon = Polygon(((sw, se, ne, nw, sw),))

    return Feature(geometry=polygon, properties={
        "id": row["id"],
        "value": row["value"]
    })


def load_data(data_dir: str) -> pd.DataFrame:
    files = glob.glob(os.path.join(data_dir, '*.txt'))

    data_frames = []
    # Load all files
    for file in tqdm.tqdm(files, desc="load files"):
        parts = file.split(os.path.sep)[-1].split('_')
        scenario = parts[4]
        variable = parts[0]
        timeframe = 'year'
        aggregation = parts[-1].split('.')[0]  # mean, min, max

        # Ignore aggregations other than mean for now
        if aggregation != 'mean':
            continue

        # The file names are insane. We have to do weird shit here. Sorry.
        if 'huglin' in file:
            variable = 'huglin'
        elif 'vp_vernal' in file:
            if 'duration' in file:
                variable = 'vp_vernal_duration'
            elif 'begin' in file:
                variable = 'vp_vernal_begin'
            else:
                print('Could not find correct vp_vernal variable for', file, '. skipping...')
                continue
        elif 'vp_frostvernal_pfrost' in file:
            variable = 'vp_frostvernal_pfrost'
        elif 'vp_frostvernal_dfrost' in file:
            variable = 'vp_frostvernal_dfrost'
        elif 'tmin_lt_0' in file:
            if 'lastday' in file:
                variable = 'tmin_lt_0_lastday'
            else:
                variable = 'tmin_lt_0'
        elif 'tmin_ge_20' in file:
            variable = 'tmin_ge_20'
        elif 'tmax_lt_0' in file:
            variable = 'tmax_lt_0'
        elif 'tmax_ge_25' in file:
            variable = 'tmax_ge_25'
        elif 'tmax_ge_30' in file:
            variable = 'tmax_ge_30'
        elif 'pre_lt_01mm' in file:
            # Trockentage wurden letztendlich auch aus dem Klimabericht genommen
            variable = 'pre_lt_01mm'
            continue
        elif 'pre_ge_01mm' in file:
            variable = 'pre_ge_01mm'
        elif 'pre_ge_10mm' in file:
            variable = 'pre_ge_10mm'
        elif 'pre_ge_20mm' in file:
            variable = 'pre_ge_20mm'
        elif 'martonne' in file:
            variable = 'martonne'
            timeframe = parts[-2]
        elif 'drought_index' in file:
            if 'avg' in file:
                variable = 'drought_index_avg'
            elif 'max' in file:
                variable = 'drought_index_max'
            elif 'qty' in file:
                variable = 'drought_index_qty'
            else:
                print('Could not find correct drought_index variable for', file, '. skipping...')
                continue
        else:
            # This is for tas and pr where we have data for seasons
            timeframe = parts[-2]  # year, djf, jja, mam, son

        variable = variable + '-' + timeframe

        data = pd.read_csv(file, skipinitialspace=True).dropna(axis=1)
        # Store the row number as the id for each cell/row
        data['id'] = range(len(data))  # data.index
        # Reduce the number of decimal places (especially for gps coordinates) to reduce size in json
        data = data.round(4)

        data = data.melt(['id', 'lat', 'lon'], var_name='timerange', value_name='value')
        data['variable'] = variable
        data['scenario'] = scenario

        data_frames.append(data)

    all_data = pd.concat(data_frames, axis=0)

    # this significantly speeds up indexing
    all_data["timerange"] = all_data["timerange"].astype("category")
    all_data["variable"] = all_data["variable"].astype("category")
    all_data["scenario"] = all_data["scenario"].astype("category")

    return all_data


def load_meta(data_frame) -> Tuple[List, List, List]:
    scenarios = data_frame.scenario.unique().sort_values().tolist()
    timeranges = data_frame.timerange.unique().sort_values().tolist()

    with open("variables.json", "r") as file:
        variables = ujson.load(file)

    for variable, meta in variables.items():
        min_value = data_frame[data_frame.variable == variable].value.min().item()
        max_value = data_frame[data_frame.variable == variable].value.max().item()
        if not meta["min"] or meta["min"] > min_value:
            meta["min"] = min_value
        if not meta["max"] or meta["max"] < max_value:
            meta["max"] = max_value

    variables = [{"var_id": k, **v} for k, v in variables.items()]

    return scenarios, variables, timeranges
