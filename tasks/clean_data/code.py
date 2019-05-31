import pandas as pd
import csv
import sys
import json
import logging
import time
import requests
# custom libs
from performance_monitor import PerformanceMonitor as PerformanceMonitor
from store import Store as Store
from credentials import credentials as credentials

def get_args_params():
    return { 'pipeline_id': 'pipeline_linear_test' }
    args = sys.argv
    print(args)
    if args is not None:
        try:
            return json.loads(args[1])
        except ValueError:
            print('Failed to parse args.')
            return {}
    return {}


def save_result(df, data_path):
    df.to_csv(data_path, sep=',', encoding='utf-8')


def main():
    params = get_args_params()
    pm = PerformanceMonitor(task_name='clean_data', pipeline_id=params['pipeline_id'])
    store = Store(performance_monitor=pm, bucket_name='forecasting-pipeline-files')
    pm.start()
    url = credentials['elasticsearch_host'] + ':' + credentials['elasticsearch_port']

    # Init elasticsearch indexes
    requests.put(url + '/metrics')
    time.sleep(10)
    try:
        quality_setting = 'high'
        quality_presets_path = 'config/quality.json'
        original_data_path = 'data/train_original.csv'
        transformed_data_path = 'data/train.csv'
        # params = get_args_params()
        # print(params)
        presets = store.get_json(quality_presets_path)
        preset = presets[quality_setting]
        df = store.get_csv(original_data_path)
        df[preset['y']] = df['volume'] * df['unit_price']
        save_result(df, transformed_data_path.split('/')[-1])
        store.save_file(transformed_data_path)
        pm.stop()
    except Exception as e:
        logging.fatal(e, exc_info=True)
        pm.stop()

if __name__ == "__main__":

    main()