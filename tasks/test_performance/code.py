import pandas as pd
import csv
import sys
import json

def get_args_params():
    args = sys.argv
    print(args)
    if args is not None:
        try:
            return json.loads(args[1])
        except ValueError:
            print('Failed to parse args.')
            return {}
    return {}


def get_quality_preset(quality_presets_path, params):
    try:
        print(quality_presets_path)
        print(params)
        with open(quality_presets_path, 'r') as f:
            presets = json.load(f)
            return presets[params['quality_setting']]
    except FileNotFoundError:
        return {}


# def pass_args_params(args):
#     result_json = json.dumps(args)
#     with open('/tmp/result.json', 'w') as file:
#         file.write(result_json)

#     with tarfile.open('/tmp/result.tgz', "w:gz") as tar:
#         abs_path = os.path.abspath('/tmp/result.json')
#         tar.add(abs_path, arcname=os.path.basename('/tmp/result.json'), recursive=False)


def get_data(data_path):
    try:
        with open(data_path, 'r') as f:
            return pd.read_csv(f, delimiter=',')
    except FileNotFoundError:
        return {}


def save_result(df, data_path):
    df.to_csv(data_path, sep=',', encoding='utf-8')


def main():
    quality_presets_path = '/config/quality.json'
    original_data_path = '/data/train_original.csv'
    transformed_data_path = '/data/train.csv'
    params = get_args_params()
    print(params)
    quality_preset = get_quality_preset(quality_presets_path, params)
    print(quality_preset)
    df = get_data(original_data_path)
    print('df')
    print(df)
    df[quality_preset['y']] = df['volume'] * df['unit_price']
    save_result(df, transformed_data_path)
    # pass_args_params(args)

if __name__ == "__main__":

    main()