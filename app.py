# import argparse
import os
import pandas as pd
from geojson import GeometryCollection, Feature, FeatureCollection, Polygon
from flask import Flask, request, Response, jsonify

from model import CSVDict, DictKey

# parser = argparse.ArgumentParser()
# parser.add_argument('-p', '--path', type=str, help='Path to folder with CSV files to serve.')
# args = parser.parse_args()
path = os.environ['BDATG_REST_API_PATH']

csv_dict = CSVDict(path)

app = Flask(__name__)


def build_feature(row: pd.Series, prop_name: str = 'temperature') -> Feature:
    loc = (row['lat'], row['lon'])
    # Find column that is not 'lat' or 'lon', since this one contains the data
    data_col = [c for c in row.index if c not in ['lat', 'lon']][0]
    geom = Polygon([[(loc[0] - 0.00005, loc[1] - 0.00005),
                     (loc[0] + 0.00005, loc[1] - 0.00005),
                     (loc[0] + 0.00005, loc[1] + 0.00005),
                     (loc[0] - 0.00005, loc[1] + 0.00005),
                     (loc[0] - 0.00005, loc[1] - 0.00005)]])
    return Feature(geometry=geom, properties={prop_name: row[data_col]})


def pandas_to_geojson(data: pd.DataFrame, prop_name: str = 'temperature') -> FeatureCollection:
    features = data.apply(lambda row: build_feature(row, prop_name=prop_name), axis=1).tolist()
    return FeatureCollection(features)


@app.route('/all_locations/<scenario>/<var>/<timerange>', methods=['GET'])
def all_locations(scenario: str, var: str, timerange: str) -> Response:
    key = DictKey(var=var, scenario=scenario)
    data = csv_dict[key]
    return pandas_to_geojson(data[['lon', 'lat', timerange]], prop_name=var)

# @app.route('/time', methods=['GET'])
# def time() -> Response:

# if __name__ == "__main__":
