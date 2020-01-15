# import argparse
import os
import glob
import time
from collections import OrderedDict
from sqlalchemy import create_engine, Table, MetaData, func
import pandas as pd
from geojson import GeometryCollection, Feature, FeatureCollection, Point  # Polygon
from flask import Flask, request, Response, jsonify, abort
from flask_caching import Cache
from flask_compress import Compress
from flask_cors import CORS
from tqdm import tqdm

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

var_dict = OrderedDict([
    ('pr-year', {
        'var': 'Mittlerer Jahresniederschlag',
        'unit': 'mm/qm',
        'description': 'Absolute Niederschlagssumme (Regen und Schnee) in mm',
        'colormap': 'YlGnBu',
        'min': float('inf'),
        'max': -float('inf')
    }),
    ('pr-djf', {
        'var': 'Mittlerer Niederschlag (Winter)',
        'unit': 'mm/qm',
        'description': 'Absolute Niederschlagssumme (Regen und Schnee) in mm zwischen Dezember und Februar',
        'colormap': 'YlGnBu',
        'min': float('inf'),
        'max': -float('inf')
    }),
    ('pr-mam', {
        'var': 'Mittlerer Niederschlag (Frühjahr)',
        'unit': 'mm/qm',
        'description': 'Absolute Niederschlagssumme (Regen und Schnee) in mm zwischen März und Mai',
        'colormap': 'YlGnBu',
        'min': float('inf'),
        'max': -float('inf')
    }),
    ('pr-jja', {
        'var': 'Mittlerer Niederschlag (Sommer)',
        'unit': 'mm/qm',
        'description': 'Absolute Niederschlagssumme (Regen und Schnee) in mm zwischen Juni und August',
        'colormap': 'YlGnBu',
        'min': float('inf'),
        'max': -float('inf')
    }),
    ('pr-son', {
        'var': 'Mittlerer Niederschlag (Herbst)',
        'unit': 'mm/qm',
        'description': 'Absolute Niederschlagssumme (Regen und Schnee) in mm zwischen September und November',
        'colormap': 'YlGnBu',
        'min': float('inf'),
        'max': -float('inf')
    }),
    ('tas-year', {
        'var': 'Mittlere Jahrestemperatur',
        'unit': '°C',
        'description': 'Mittlere Lufttemperatur (in 2 Meter Höhe) in °C',
        'colormap': 'Turbo',
        'min': float('inf'),
        'max': -float('inf')
    }),
    ('tas-djf', {
        'var': 'Mittlere Temperatur (Winter)',
        'unit': '°C',
        'description': 'Mittlere Lufttemperatur (in 2 Meter Höhe) in °C zwischen Dezember und Februar',
        'colormap': 'Turbo',
        'min': float('inf'),
        'max': -float('inf')
    }),
    ('tas-mam', {
        'var': 'Mittlere Temperatur (Frühjahr)',
        'unit': '°C',
        'description': 'Mittlere Lufttemperatur (in 2 Meter Höhe) in °C zwischen März und Mai',
        'colormap': 'Turbo',
        'min': float('inf'),
        'max': -float('inf')
    }),
    ('tas-jja', {
        'var': 'Mittlere Temperatur (Sommer)',
        'unit': '°C',
        'description': 'Mittlere Lufttemperatur (in 2 Meter Höhe) in °C zwischen Juni und August',
        'colormap': 'Turbo',
        'min': float('inf'),
        'max': -float('inf')
    }),
    ('tas-son', {
        'var': 'Mittlere Temperatur (Herbst)',
        'unit': '°C',
        'description': 'Mittlere Lufttemperatur (in 2 Meter Höhe) in °C zwischen September und November',
        'colormap': 'Turbo',
        'min': float('inf'),
        'max': -float('inf')
    }),
    ('vp_vernal-year', {
        'var': 'Thermische Vegetationsperiode',
        'unit': 'Tage',
        'description': """ Die thermische Vegetationsperiode eines Jahres ist definiert als die Anzahl Tage zwischen
<br>
1.) Vegetationsbeginn:  Erstes Aufkommen von mindestens 6 aufeinanderfolgenden Tagen mit einer Durchschnittstemperatur über 5°C und
<br>
2.) Vegetationsende:    Erstes Aufkommen von mindestens 6 aufeinanderfolgenden Tagen mit einer Durchschnittstemperatur unter 5°C im Winterhalbjahr. """,
        'colormap': 'Warm',
        'min': float('inf'),
        'max': -float('inf')
    }),
    ('huglin-year', {
        'var': 'Huglin Index',
        'unit': '',
        'description': """ Ein für den Weinbau konzipierter Wärmesummenindex aus Tagesmittel- und Maximumtemperaturen im Sommerhalbjahr (April-September).
<table class="table table-striped table-bordered">
<thead>
<tr>
<th><em>Huglin-Index H</em></th>
<th><em>Rebsorten</em></th>
</tr>
</thead>
<tbody>
<tr>
<td>H &lt; 1500</td>
<td>keine Anbauempfehlung</td>
</tr>
<tr>
<td>1500 ≤ H &lt; 1600</td>
<td><a href="https://de.wikipedia.org/wiki/M%C3%BCller_Thurgau">Müller Thurgau</a></td>
</tr>
<tr>
<td>1600 ≤ H &lt; 1700</td>
<td><a href="https://de.wikipedia.org/wiki/Pinot_Blanc">Pinot Blanc</a> ,  <a href="https://de.wikipedia.org/wiki/Grauer_Burgunder">Grauer Burgunder</a> ,  <a href="https://de.wikipedia.org/wiki/Aligot%C3%A9">Aligoté</a> ,  <a href="https://de.wikipedia.org/wiki/Gamay">Gamay</a>  Noir,  <a href="https://de.wikipedia.org/wiki/Gew%C3%BCrztraminer">Gewürztraminer</a></td>
</tr>
<tr>
<td>1700 ≤ H &lt; 1800</td>
<td><a href="https://de.wikipedia.org/wiki/Riesling">Riesling</a> ,  <a href="https://de.wikipedia.org/wiki/Chardonnay">Chardonnay</a> ,  <a href="https://de.wikipedia.org/wiki/Silvaner">Silvaner</a> ,  <a href="https://de.wikipedia.org/wiki/Sauvignon_Blanc">Sauvignon Blanc</a> ,  <a href="https://de.wikipedia.org/wiki/Pinot_Noir">Pinot Noir</a> ,  <a href="https://de.wikipedia.org/wiki/Gr%C3%BCner_Veltliner">Grüner Veltliner</a></td>
</tr>
<tr>
<td>1800 ≤ H &lt; 1900</td>
<td><a href="https://de.wikipedia.org/wiki/Cabernet_Franc">Cabernet Franc</a></td>
</tr>
<tr>
<td>1900 ≤ H &lt; 2000</td>
<td><a href="https://de.wikipedia.org/wiki/Chenin_Blanc">Chenin Blanc</a> ,  <a href="https://de.wikipedia.org/wiki/Cabernet_Sauvignon">Cabernet Sauvignon</a> ,  <a href="https://de.wikipedia.org/wiki/Merlot">Merlot</a> ,  <a href="https://de.wikipedia.org/wiki/Semillion">Semillion</a> ,  <a href="https://de.wikipedia.org/wiki/Welschriesling">Welschriesling</a></td>
</tr>
<tr>
<td>2000 ≤ H &lt; 2100</td>
<td><a href="https://de.wikipedia.org/wiki/Ugni_Blanc">Ugni Blanc</a></td>
</tr>
<tr>
<td>2100 ≤ H &lt; 2200</td>
<td><a href="https://de.wikipedia.org/wiki/Grenache">Grenache</a> ,  <a href="https://de.wikipedia.org/wiki/Syrah">Syrah</a> ,  <a href="https://de.wikipedia.org/wiki/Cinsaut">Cinsaut</a></td>
</tr>
<tr>
<td>2200 ≤ H &lt; 2300</td>
<td><a href="https://de.wikipedia.org/wiki/Carignan_(Rebsorte)">Carignan</a></td>
</tr>
<tr>
<td>2300 ≤ H &lt; 2400</td>
<td><a href="https://de.wikipedia.org/wiki/Aramon">Aramon</a></td>
</tr>
</tbody>
</table>

Quelle für Tabelle:  <a href="https://www.pik-potsdam.de/research/publications/pikreports/.files/pr106.pdf">Stock, M. et al. (2007) Perspektiven der Klimaänderung bis 2050 für den Weinbau in Deutschland (Klima 2050). Schlußbericht zum FDW-Vorhaben Klima 2050. PIK Report 106.</a> """
        ,
        'colormap': 'Warm',
        'min': float('inf'),
        'max': -float('inf')
    }),
    ('tmin_lt_0-year', {
        'var': 'Frosttage',
        'unit': 'Tage',
        'description': 'Anzahl der Tage, an denen die minimale Lufttemperatur unter 0°C sinkt',
        'colormap': 'Warm',
        'min': float('inf'),
        'max': -float('inf')
    })
])

scenarios = set()
variables = var_dict.keys()  # set()
timeranges = set()


def init() -> None:

    files = glob.glob(os.path.join(path, '*.txt'))

    # Load all files
    for f in tqdm(files):
        parts = f.split(os.path.sep)[-1].split('_')
        scenario = parts[4]
        variable = parts[0]
        timeframe = 'year'
        aggregation = parts[-1].split('.')[0]  # mean, min, max

        # Ignore aggregations other than mean for now
        if aggregation != 'mean':
            continue

        # The file names are insane. We have to do weird shit here. Sorry.
        if 'huglin' in f:
            variable = 'huglin'
        elif 'vp_vernal' in f:
            variable = 'vp_vernal'
        elif 'tmin_lt_0' in f:
            variable = 'tmin_lt_0'
        else:
            # This is for tas and pr where we have data for seasons
            timeframe = parts[-2]  # year, djf, jja, mam, son

        variable = variable + '-' + timeframe

        data = pd.read_csv(f, skipinitialspace=True).dropna(axis=1)
        # Store the row number as the id for each cell/row
        data['id'] = range(len(data))  # data.index
        # Reduce the number of decimal places (especially for gps coordinates) to reduce size in json
        data = data.round(4)

        data = data.melt(['id', 'lat', 'lon'], var_name='timerange', value_name='value')
        data['var'] = variable
        data['scenario'] = scenario

        scenarios.add(scenario)
        # variables.add(variable)
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

    if scenario not in scenarios or var not in variables or timerange not in timeranges:
        abort(404)

    data = pd.read_sql(data_table.select().where(data_table.c.var == var)
                                          .where(data_table.c.scenario == scenario)
                                          .where(data_table.c.timerange == timerange), con=engine)
    return pandas_to_geojson(data, data_col='value', prop_name=var)


@app.route('/all_times/<int:cell_id>/<scenario>/<var>', methods=['GET'])
@cache.cached()
def all_times(cell_id: int, scenario: str, var: str) -> Response:

    if scenario not in scenarios or var not in variables:
        abort(404)

    data = pd.read_sql(data_table.select().where(data_table.c.var == var)
                                          .where(data_table.c.scenario == scenario)
                                          .where(data_table.c.id == cell_id)
                                          .order_by(data_table.c.timerange), con=engine)

    if data.shape[0] == 0:
        abort(404)

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
