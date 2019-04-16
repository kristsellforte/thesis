import pickle
import json
import tarfile
import os
from datetime import datetime
import pandas as pd
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score

def get_scores(scores_path):
    try:
        with open(scores_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return { '0': {} }


def get_model(scores):
    version_number = str(int(max(scores.keys())) + 1)
    model_path = './models/' + version_number + '.pkl'
    model = pickle.load(open(model_path, 'rb'))

    return model


def score_model(model, data_path):
    # primitive scoring function
    df = pd.read_csv(data_path)
    df['date'] = df['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').toordinal())
    x = df[['date', 'store', 'item']]
    y = df[['sales']]
    r2_squared_score = model.score(x, y)

    return { 'r2_squared': r2_squared_score }


def save_score(score, scores_path):
    with open(scores_path, 'r+') as scores_json:
        scores = json.load(scores_json)
        version_number = str(int(max(scores.keys())) + 1)
        scores_json.seek(0)
        scores[version_number] = score
        json.dump(scores, scores_json, indent=4)

        # save tar for sharing across airflow tasks
        with tarfile.open('/tmp/result.tgz', "w:gz") as tar:
            abs_path = os.path.abspath(scores_path)
            tar.add(abs_path, arcname=os.path.basename(scores_path), recursive=False)


def main():
    scores_path = './scores.json'
    data_path = './data.csv'
    scores = get_scores(scores_path)
    model = get_model(scores)
    model_score = score_model(model, data_path)
    save_score(model_score, scores_path)

if __name__ == "__main__" :

    main()