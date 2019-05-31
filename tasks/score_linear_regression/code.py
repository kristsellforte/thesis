import pickle
import json
import tarfile
import os
import sys
from datetime import datetime
import pandas as pd
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score
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


def get_version_number(scores):
    return str(max(map(int, scores.keys())) + 1)


def score_model(store, model, data_path, quality_preset):
    # primitive scoring function
    df = store.get_csv(data_path)
    df['date'] = df['date'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').toordinal())
    x_columns = list(df.columns)
    x_columns.remove('sales')
    x = df[x_columns]
    y = df[quality_preset['y']]
    r2_squared_score = model.score(x, y)

    return { 'r2_squared': r2_squared_score }


def save_score(score, scores_path):
    with open(scores_path, 'r+') as scores_json:
        scores = json.load(scores_json)
        version_number = get_version_number(scores)
        scores_json.seek(0)
        scores[version_number] = score
        json.dump(scores, scores_json, indent=4)


def main():
    params = get_args_params()
    pm = PerformanceMonitor(task_name='score_linear_regression', pipeline_id=params['pipeline_id'])
    store = Store(performance_monitor=pm, bucket_name='forecasting-pipeline-files')
    pm.start()
    time.sleep(7)
    try: 
        # execution_id = get_execution_id()
        # print(f"Execution id: {execution_id}")
        quality_setting = 'high'
        scores_path = 'scores/linear_regression.json'
        data_path = 'data/train.csv'
        quality_presets_path = 'config/quality.json'

        # params = get_args_params()
        presets = store.get_json(quality_presets_path)
        preset = presets[quality_setting]
        scores = store.get_json(scores_path)
        version_number = get_version_number(scores)
        model_path = 'models/linear_regression/' + version_number + '.pkl'
        model = store.get_pickle(model_path)
        # scores = get_scores(scores_path)
        # model = get_model(scores)
        model_score = score_model(store, model, data_path, preset)
        pm.log_analytics_metric(model_score)
        save_score(model_score, scores_path.split('/')[-1])
        store.save_file(scores_path)
        pm.stop()
    except Exception as e:
        logging.fatal(e, exc_info=True)
        pm.stop()

if __name__ == "__main__" :

    main()