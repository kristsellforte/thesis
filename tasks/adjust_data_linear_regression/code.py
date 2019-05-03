import pandas as pd
import json
import csv
import tarfile
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


def get_quality_preset(quality_presets_path, params):
    try:
        with open(quality_presets_path, 'r') as f:
            presets = json.load(f)
            return presets[params['quality_setting']]
    except FileNotFoundError:
        return {}


# def save_adjustment_settings(adjustment_settings):
#     print('Saving result to /tmp/result.json')
#     result_json = json.dumps(adjustment_settings)
#     with open('/tmp/result.json', 'w') as file:
#         file.write(result_json)
        
#     with tarfile.open('/tmp/result.tgz', "w:gz") as tar:
#         abs_path = os.path.abspath('/tmp/result.json')
#         tar.add(abs_path, arcname=os.path.basename('/tmp/result.json'), recursive=False)
        
#     print('Successfully saved.')


def save_adjustment(df, data_path):
    df.to_csv(data_path, sep=',', encoding='utf-8', index=False)


def save_quality_setting(model, scores):
    version_number = int(max(scores.keys())) + 1
    pickle_path = '/models/linear_regression/' + str(version_number) + '.pkl'
    pickle.dump(model, open(pickle_path, 'wb'))


def main():
    data_path = '/data/train.csv'
    quality_presets_path = '/config/quality.json'
    params = get_args_params()
    quality_preset = get_quality_preset(quality_presets_path, params)
    df = get_data(data_path)
    aggregation = quality_preset['aggregation']
    scope = aggregation + quality_preset['y'] + quality_preset['categorical']
    df = df[scope]
    df = pd.get_dummies(data=df, columns=quality_preset['categorical'])
    keys = list(df.columns)
    keys.remove(quality_preset['y'][0])
    df = df.groupby(keys).sum().reset_index()
    save_adjustment(df, data_path)


if __name__ == "__main__":

    main()