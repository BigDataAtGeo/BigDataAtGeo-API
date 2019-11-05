# import argparse
import os
import glob
import time
import pandas as pd
from geojson import GeometryCollection, Feature, FeatureCollection, Point  # Polygon
from flask import Flask, request, Response, jsonify
from flask_caching import Cache
from flask_compress import Compress
from flask_cors import CORS

from model import CSVDict, DictKey

# parser = argparse.ArgumentParser()
# parser.add_argument('-p', '--path', type=str, help='Path to folder with CSV files to serve.')
# args = parser.parse_args()
path = os.environ['BDATG_REST_API_PATH']

csv_dict = CSVDict(path)

config = {
    'DEBUG': False,
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 60*60*24
}

app = Flask(__name__)
app.config.from_mapping(config)
cache = Cache(app)
Compress(app)
CORS(app)

var_dict = {
    'pr': 'Mittlerer Jahresniederschlag',
    'tas': 'Mittlere Jahrestemperatur'
}


def build_feature(row: pd.Series, data_col: str, prop_name: str = 'temperature') -> Feature:
    loc = (row['lon'], row['lat'])
    geom = Point((loc[0], loc[1]))
    f = Feature(geometry=geom, properties={
        'id': row['id'],
        'value': row[data_col]
        })
    return f


def pandas_to_geojson(data: pd.DataFrame, data_col: str, prop_name: str = 'temperature') -> FeatureCollection:
    features = data.apply(lambda row: build_feature(row, data_col=data_col, prop_name=prop_name), axis=1).tolist()
    # features = [build_feature(row, data_col=data_col, prop_name=prop_name) for _, row in data.iterrows()]
    return FeatureCollection(features)


@app.route('/all_locations/<scenario>/<var>/<timerange>', methods=['GET'])
@cache.cached()
def all_locations(scenario: str, var: str, timerange: str) -> Response:
    key = DictKey(var=var, scenario=scenario)
    data = csv_dict[key]
    return pandas_to_geojson(data[['id', 'lon', 'lat', timerange]], data_col=timerange, prop_name=var)


@app.route('/all_times/<int:cell_id>/<scenario>/<var>', methods=['GET'])
@cache.cached()
def all_times(cell_id: int, scenario: str, var: str) -> Response:
    key = DictKey(var=var, scenario=scenario)
    data = csv_dict[key]
    row = data.iloc[cell_id]
    keys = sorted([c for c in row.index if c not in ['id', 'lat', 'lon']])

    resp = {
        'lat': row['lat'],
        'lon': row['lon'],
        'data': {
            'keys': keys,
            'values':  row[keys].to_list()
        }
    }
    return jsonify(resp)


@app.route('/index', methods=['GET'])
@cache.cached()
def index() -> str:
    files = glob.glob(os.path.join(path, '*.txt'))

    # We assume that each file has the same timeranges for now
    file_headers = open(files[0]).readline().rstrip()
    timeranges = [h.strip() for h in file_headers.split(',')[2:-1]]

    files = [f.split(os.path.sep)[-1].split('_') for f in files]
    # Find all unique scenarios
    scenarios = list(set([f[4] for f in files]))
    # Find all unique variables
    variables = list(set([f[0] for f in files]))
    variables = [{'var_id': v, 'var': var_dict[v]} for v in variables]

    file_infos = {
                    'variables': variables,
                    'scenarios': scenarios,
                    'timeranges': timeranges
                  }

    return jsonify(file_infos)

# @app.route('/index', methods=['GET'])
# @cache.cached()
# def index() -> str:
#     files = glob.glob(os.path.join(path, '*.txt'))

#     file_headers = [open(f).readline().rstrip() for f in files]
#     files = [f.split(os.path.sep)[-1].split('_') for f in files]

#     files = zip(files, file_headers)

#     file_infos = [{
#                     'var_id': f[0],
#                     'var': var_dict[f[0]],
#                     'scenario': f[4],
#                     'timeranges': [h.strip() for h in h.split(',')[2:-1]]
#                   } for f, h in files]

#     return jsonify(file_infos)

# @app.route('/time', methods=['GET'])
# def time() -> Response:

# if __name__ == "__main__":
