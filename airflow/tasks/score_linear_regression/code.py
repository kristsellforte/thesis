import pickle
import json
import tarfile
import os
from datetime import datetime
import pandas as pd
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score

def get_pulled_model(scores):
    print(scores)
    version_number = get_version_number(scores)
    print(version_number)
    model_path = version_number + '.pkl'
    model = pickle.load(open(model_path, 'rb'))
    print(model)

    return model

    # with open("params.yaml", 'r') as f:
        # return yaml.safe_load(f)

def get_yaml_params():
    try:
        with open("params.yaml", 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        return {}


def get_args_params():
    args = sys.argv
    print(f"Args are {args}")
    if args is not None:
        try:
            return json.loads(args[1])
        except ValueError:
            print('Failed to parse args.')
            return {}
    return {}


def get_execution_id():
    print('env')
    print(os.environ)
    print(os.environ['result'])
    print(os.environ['task'])
    # for task_id in context['task'].upstream_task_ids:
        # pulled_data[task_id] = context['task_instance'].xcom_pull(task_ids=task_id, key='data')
    
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
    execution_id = get_execution_id()
    print(f"Execution id: {execution_id}")
    # yaml_params = get_yaml_params()
    # print(f"Yaml params are {yaml_params}")

    # arg_params = get_args_params()
    # print(f"Arg Params are {arg_params}")
    scores_path = './tmp/scores.json'
    data_path = './tmp/data.csv'
    scores = get_scores(scores_path)
    # pulled_model = get_pulled_model(scores)
    model = get_model(scores)
    print(model)
    # print(pulled_model)
    model_score = score_model(model, data_path)
    save_score(model_score, scores_path)

if __name__ == "__main__" :

    main()