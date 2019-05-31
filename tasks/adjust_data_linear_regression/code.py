import pandas as pd
import json
import csv
import tarfile
import sys
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


def save_adjustment(df, data_path):
    df.to_csv(data_path, sep=',', encoding='utf-8', index=False)


def save_quality_setting(model, scores):
    version_number = int(max(scores.keys())) + 1
    pickle_path = '/models/linear_regression/' + str(version_number) + '.pkl'
    pickle.dump(model, open(pickle_path, 'wb'))


def main():
    params = get_args_params()
    pm = PerformanceMonitor(task_name='adjust_data_linear_regression', pipeline_id=params['pipeline_id'])
    store = Store(performance_monitor=pm, bucket_name='forecasting-pipeline-files')
    pm.start()
    time.sleep(8)
    try: 
        quality_setting = 'high'
        data_path = 'data/train.csv'
        quality_presets_path = 'config/quality.json'
        # params = get_args_params()
        presets = store.get_json(quality_presets_path)
        # print(presets)
        preset = presets[quality_setting]
        # quality_preset = get_quality_preset(quality_presets_path, params)
        df = store.get_csv(data_path)
        aggregation = preset['aggregation']
        scope = aggregation + preset['y'] + preset['categorical']
        df = df[scope]
        df = pd.get_dummies(data=df, columns=preset['categorical'])
        keys = list(df.columns)
        keys.remove(preset['y'][0])
        df = df.groupby(keys).sum().reset_index()
        save_adjustment(df, data_path.split('/')[-1])
        store.save_file(data_path)
        pm.stop()
    except Exception as e:
        logging.fatal(e, exc_info=True)
        pm.stop()


if __name__ == "__main__":

    main()