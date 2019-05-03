from datetime import datetime
import pandas as pd
from fbprophet import Prophet

import pickle
import json
import os
import sys

def get_args_params():
    args = sys.argv
    if args is not None:
        try:
            return json.loads(args[1])
        except ValueError:
            print('Failed to parse args.')
            return {}
    return {}


def get_data(data_path):
    try:
        with open(data_path, 'r') as f:
            return pd.read_csv(f, delimiter=',')
    except FileNotFoundError:
        return {}


def transform_data(df, quality_preset):
    df['ds'] = df['date']
    df['y'] = df['sales']
    df = df[['ds', 'y'] + quality_preset['categorical']]

    return df


def train_model(df):
    model = Prophet()
    model.fit(df)

    return model


def get_scores(scores_path):
    try:
        with open(scores_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return { '0': {} }


def get_version_number(scores):
    return int(max(scores.keys())) + 1


def save_model(model, models_path, postfix):
    pickle_path = models_path + '/' + postfix + '.pkl'
    pickle.dump(model, open(pickle_path, 'wb'))


def save_scores(empty_scores, scores_path):
    with open(scores_path, 'r+') as scores_json:
        scores_json.seek(0)
        json.dump(empty_scores, scores_json, indent=4)

def get_quality_preset(quality_presets_path, params):
    try:
        with open(quality_presets_path, 'r') as f:
            presets = json.load(f)
            return presets[params['quality_setting']]
    except FileNotFoundError:
        return {}


def main():
    scores_path = '/scores/prophet.json'
    data_path = '/data/train.csv'
    quality_presets_path = '/config/quality.json'
    params = get_args_params()
    quality_preset = get_quality_preset(quality_presets_path, params)
    df = get_data(data_path)
    df = transform_data(df, quality_preset)
    scores = get_scores(scores_path)
    version_number = get_version_number(scores)
    models_path = '/models/prophet/' + str(version_number)
    os.mkdir(models_path)
    scores[str(version_number)] = {}

    if len(quality_preset['categorical']) != 0:
        col1 = quality_preset['categorical'][0]
        for val1 in df[col1].unique():
            df_val1 = df[df[col1] == val1]
            # if not bool(scores[str(version_number)]) or bool(scores[str(version_number)][str(col1) + '_' + str(val1)]):
            scores[str(version_number)][str(col1) + '_' + str(val1)] = {}
            col2 = quality_preset['categorical'][1]
            for val2 in df_val1[col2].unique():
                df_val2 = df_val1[df_val1[col2] == val2]
                model = train_model(df_val2)
                scores[str(version_number)][str(col1) + '_' + str(val1)][str(col2) + '_' + str(val2)] = {}
                model_postfix = str(col1) + '_' + str(val1) + '_' + str(col2) + '_' + str(val2)
                save_model(model, models_path, model_postfix)
    else:
        model = train_model(df)
        scores[str(version_number)] = {}
        save_model(model, models_path, 'model')

    save_scores(scores, scores_path)
    

if __name__ == "__main__":

    main()
