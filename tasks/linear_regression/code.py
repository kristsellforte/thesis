import pandas as pd
from sklearn import linear_model
import statsmodels.api as sm
import json
import pickle
import tarfile
import os
import sys
from datetime import datetime

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
            return pd.read_csv(f)
    except FileNotFoundError:
        return {}


def get_quality_preset(quality_presets_path, params):
    try:
        with open(quality_presets_path, 'r') as f:
            presets = json.load(f)
            return presets[params['quality_setting']]
    except FileNotFoundError:
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


def get_scores(scores_path):
    try:
        with open(scores_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return { '0': {} }


def save_model(model, scores):
    version_number = int(max(scores.keys())) + 1
    pickle_path = '/models/linear_regression/' + str(version_number) + '.pkl'
    pickle.dump(model, open(pickle_path, 'wb'))

    # save tar for sharing across airflow tasks
    # with tarfile.open('/tmp/result.tgz', "w:gz") as tar:
    #     abs_path = os.path.abspath(pickle_path)
    #     tar.add(abs_path, arcname=os.path.basename(pickle_path), recursive=False)


def main():
    scores_path = '/scores/linear_regression.json'
    data_path = '/data/train.csv'
    quality_presets_path = '/config/quality.json'
    params = get_args_params()
    quality_preset = get_quality_preset(quality_presets_path, params)
    df = get_data(data_path)
    x, y = prepare_data(df, quality_preset)
    model = linear_regression_sklearn(x, y)
    scores = get_scores(scores_path)
    save_model(model, scores)


if __name__ == "__main__":

    main()
