import pandas as pd
from sklearn import linear_model
import statsmodels.api as sm
import json
import pickle
import tarfile
import os
import sys
from datetime import datetime
import logging
import time
# custom libs
from performance_monitor import PerformanceMonitor as PerformanceMonitor
from store import Store as Store

def get_args_params():
    args = sys.argv
    if args is not None:
        try:
            return json.loads(args[1])
        except ValueError:
            print('Failed to parse args.')
            return {}
    return {}


def prepare_data(df, quality_preset):
    df['date'] = df['date'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').toordinal())
    x_columns = list(df.columns)
    x_columns.remove('sales')
    x = df[x_columns]
    y = df[quality_preset['y']]

    return (x, y)


def linear_regression_sklearn(x, y):
    regr = linear_model.LinearRegression()
    regr.fit(x, y)
    print('Intercept: \n', regr.intercept_)
    print('Coefficients: \n', regr.coef_)

    return regr


def save_model(model, path):
    pickle.dump(model, open(path.split('/')[-1], 'wb'))


def main():
    params = get_args_params()
    pm = PerformanceMonitor(task_name='linear_regression', pipeline_id=params['pipeline_id'])
    store = Store(performance_monitor=pm, bucket_name='forecasting-pipeline-files')
    pm.start()
    time.sleep(5)
    try:
        quality_setting = 'high'
        scores_path = 'scores/linear_regression.json'
        data_path = 'data/train.csv'
        quality_presets_path = 'config/quality.json'
        presets = store.get_json(quality_presets_path)
        preset = presets[quality_setting]
        df = store.get_csv(data_path)
        x, y = prepare_data(df, preset)
        model = linear_regression_sklearn(x, y)
        scores = store.get_json(scores_path)
        version_number = max(map(int, scores.keys())) + 1
        pickle_path = 'models/linear_regression/' + str(version_number) + '.pkl'
        save_model(model, pickle_path)
        store.save_file(pickle_path)
        pm.stop()
    except Exception as e:
        logging.fatal(e, exc_info=True)
        pm.stop()


if __name__ == "__main__":

    main()
