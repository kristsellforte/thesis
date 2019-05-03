import pickle
import json
import tarfile
import os
import sys
from datetime import datetime
import pandas as pd
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score

# def get_pulled_model(scores):
#     print(scores)
#     version_number = get_version_number(scores)
#     print(version_number)
#     model_path = version_number + '.pkl'
#     model = pickle.load(open(model_path, 'rb'))
#     print(model)

#     return model

def get_args_params():
    args = sys.argv
    if args is not None:
        try:
            return json.loads(args[1])
        except ValueError:
            print('Failed to parse args.')
            return {}
    return {}


def get_quality_preset(quality_presets_path, params):
    try:
        with open(quality_presets_path, 'r') as f:
            presets = json.load(f)
            return presets[params['quality_setting']]
    except FileNotFoundError:
        return {}


def get_execution_id():    
    try:
        return os.environ['EXECUTION_ID']
    except KeyError:
        return 0


def get_scores(scores_path):
    try:
        with open(scores_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return { '0': {} }


def get_version_number(scores):
    return str(int(max(scores.keys())) + 1)


def get_model(scores):
    version_number = get_version_number(scores)
    model_path = '/models/linear_regression/' + version_number + '.pkl'
    model = pickle.load(open(model_path, 'rb'))

    return model


def score_model(model, data_path, quality_preset):
    # primitive scoring function
    df = pd.read_csv(data_path)
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
        version_number = str(int(max(scores.keys())) + 1)
        scores_json.seek(0)
        scores[version_number] = score
        json.dump(scores, scores_json, indent=4)


def main():
    execution_id = get_execution_id()
    print(f"Execution id: {execution_id}")
    scores_path = '/scores/linear_regression.json'
    data_path = '/data/train.csv'
    quality_presets_path = '/config/quality.json'
    params = get_args_params()
    quality_preset = get_quality_preset(quality_presets_path, params)
    scores = get_scores(scores_path)
    model = get_model(scores)
    model_score = score_model(model, data_path, quality_preset)
    save_score(model_score, scores_path)

if __name__ == "__main__" :

    main()