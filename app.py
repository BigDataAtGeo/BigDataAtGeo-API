# import argparse
import os
import glob
import time
from sqlalchemy import create_engine, Table, MetaData, func
import pandas as pd
from geojson import GeometryCollection, Feature, FeatureCollection, Point  # Polygon
from flask import Flask, request, Response, jsonify
from flask_caching import Cache
from flask_compress import Compress
from flask_cors import CORS

# parser = argparse.ArgumentParser()
# parser.add_argument('-p', '--path', type=str, help='Path to folder with CSV files to serve.')
# args = parser.parse_args()
path = os.environ['BDATG_REST_API_PATH']

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

engine = create_engine('sqlite://', echo=False)

var_dict = {
    'pr': {
        'var': 'Mittlerer Jahresniederschlag',
        'unit': 'mm/qm',
        'description': '',
        'colormap': 'YlGnBu',
        'min': float('inf'),
        'max': -float('inf')
    },
    'tas': {
        'var': 'Mittlere Jahrestemperatur',
        'unit': '°C',
        'description': '',
        'colormap': 'Warm',
        'min': float('inf'),
        'max': -float('inf')
    }
}

scenarios = set()
variables = set()
timeranges = set()


def init() -> None:

    files = glob.glob(os.path.join(path, '*.txt'))

    # Load all files
    for f in files:
        parts = f.split(os.path.sep)[-1].split('_')
        scenario = parts[4]
        variable = parts[0]

        data = pd.read_csv(f, skipinitialspace=True).dropna(axis=1)
        # Store the row number as the id for each cell/row
        data['id'] = range(len(data))  # data.index
        # Reduce the number of decimal places (especially for gps coordinates) to reduce size in json
        data = data.round(4)

        data = data.melt(['id', 'lat', 'lon'], var_name='timerange', value_name='value')
        data['var'] = variable
        data['scenario'] = scenario

        scenarios.add(scenario)
        variables.add(variable)
        timeranges.update(list(data['timerange']))

        # Calculate Min/Max values for each variable
        min_v = data['value'].min()
        max_v = data['value'].max()

        if var_dict[variable]['min'] > min_v:
            var_dict[variable]['min'] = min_v
        if var_dict[variable]['max'] < max_v:
            var_dict[variable]['max'] = max_v

        data.to_sql('data', con=engine, index=False, if_exists='append')

    print('Ready')


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
    data = pd.read_sql(data_table.select().where(data_table.c.var == var)
                                          .where(data_table.c.scenario == scenario)
                                          .where(data_table.c.timerange == timerange), con=engine)

    return pandas_to_geojson(data, data_col='value', prop_name=var)


@app.route('/all_times/<int:cell_id>/<scenario>/<var>', methods=['GET'])
@cache.cached()
def all_times(cell_id: int, scenario: str, var: str) -> Response:
    data = pd.read_sql(data_table.select().where(data_table.c.var == var)
                                          .where(data_table.c.scenario == scenario)
                                          .where(data_table.c.id == cell_id)
                                          .order_by(data_table.c.timerange), con=engine)

    row = data.iloc[0]

    resp = {
        'lat': row['lat'],
        'lon': row['lon'],
        'data': {
            'keys': list(data['timerange']),
            'values':  list(data['value'])
        }
    }
    return jsonify(resp)


@app.route('/index', methods=['GET'])
@cache.cached()
def index() -> str:

    variables_dicts = [{'var_id': v,
                        'var': var_dict[v]['var'],
                        'unit': var_dict[v]['unit'],
                        'description': var_dict[v]['description'],
                        'colormap': var_dict[v]['colormap'],
                        'min': var_dict[v]['min'],
                        'max': var_dict[v]['max']} for v in variables]

    file_infos = {
                    'variables': variables_dicts,
                    'scenarios': list(scenarios),
                    'timeranges': sorted(list(timeranges))
                  }

    return jsonify(file_infos)


init()
data_table = Table('data', MetaData(), autoload_with=engine)


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
