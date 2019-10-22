# import argparse
import os
import time
import pandas as pd
from geojson import GeometryCollection, Feature, FeatureCollection, Point  # Polygon
from flask import Flask, request, Response, jsonify
from flask_compress import Compress

from model import CSVDict, DictKey

# parser = argparse.ArgumentParser()
# parser.add_argument('-p', '--path', type=str, help='Path to folder with CSV files to serve.')
# args = parser.parse_args()
path = os.environ['BDATG_REST_API_PATH']

csv_dict = CSVDict(path)

app = Flask(__name__)
Compress(app)


def build_feature(row: pd.Series, data_col: str, prop_name: str = 'temperature') -> Feature:
    # print('BUILD FEATURES')
    # t0 = time.time()
    loc = (row['lat'], row['lon'])
    # t1 = time.time()
    # print('resolve loc -', t1-t0)
    # Find column that is not 'lat' or 'lon', since this one contains the data
    # data_col = [c for c in row.index if c not in ['lat', 'lon']][0]
    # t2 = time.time()
    # print('find col -', t2-t1)
    geom = Point((loc[0], loc[1]))
    # geom = Polygon([[(loc[0] - 0.00005, loc[1] - 0.00005),
    #                  (loc[0] + 0.00005, loc[1] - 0.00005),
    #                  (loc[0] + 0.00005, loc[1] + 0.00005),
    #                  (loc[0] - 0.00005, loc[1] + 0.00005),
    #                  (loc[0] - 0.00005, loc[1] - 0.00005)]])
    # t2 = time.time()
    # print('Build polygon -', t2-t1)
    f = Feature(geometry=geom, properties={prop_name: row[data_col]})
    # t3 = time.time()
    # print('Build feature -', t3-t2)
    return f


def pandas_to_geojson(data: pd.DataFrame, data_col: str, prop_name: str = 'temperature') -> FeatureCollection:
    features = data.apply(lambda row: build_feature(row, data_col=data_col, prop_name=prop_name), axis=1).tolist()
    # features = [build_feature(row, data_col=data_col, prop_name=prop_name) for _, row in data.iterrows()]
    return FeatureCollection(features)


@app.route('/all_locations/<scenario>/<var>/<timerange>', methods=['GET'])
def all_locations(scenario: str, var: str, timerange: str) -> Response:
    key = DictKey(var=var, scenario=scenario)
    data = csv_dict[key]
    return pandas_to_geojson(data[['lon', 'lat', timerange]], data_col=timerange, prop_name=var)

# @app.route('/time', methods=['GET'])
# def time() -> Response:

# if __name__ == "__main__":
