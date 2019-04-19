import pandas as pd
import json
import csv

def get_data(data_path):
    try:
        with open(data_path, 'r') as f:
            return pd.read_csv(f, delimiter=';')
    except FileNotFoundError:
        return {}


def get_quality_config(quality_path):
    try:
        with open(quality_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_adjustment_settings(adjustment_settings):
    print('Saving result to /tmp/result.json')
    result_json = json.dumps(adjustment_settings)
    with open('/tmp/result.json', 'w') as file:
        file.write(result_json)
        
    with tarfile.open('/tmp/result.tgz', "w:gz") as tar:
        abs_path = os.path.abspath('/tmp/result.json')
        tar.add(abs_path, arcname=os.path.basename('/tmp/result.json'), recursive=False)
        
    print('Successfully saved.')


def save_adjustment(df, data_path):
	df.to_csv(data_path, sep='\t', encoding='utf-8')


def save_quality_setting(model, scores):
    version_number = int(max(scores.keys())) + 1
    pickle_path = '/models/linear_regression/' + str(version_number) + '.pkl'
    pickle.dump(model, open(pickle_path, 'wb'))


def main():
	data_path = '/data/train.csv'
	quality_config_path = '/config/quality.json'
	df = get_data(data_path)
	quality_config = get_quality_config(quality_config_path)['poor']
	aggregation = quality_config['aggregation']
	df['sales'] = df.volume * df.unit_price
	df = df[aggregation.extend(['sales'])]
	df = df.groupby(aggregation).sum()
	save_adjustment(df, data_path)
	save_adjustment_settings(quality_config)


if __name__ == "__main__":

    main()