import pandas as pd

def get_data(data_path):
    try:
        with open(data_path, 'r') as f:
            return pd.read_csv(f, delimiter=';')
    except FileNotFoundError:
        return {}


def get_aggregation():
    return ['date', 'location']


def save_adjustment(df):
	df.to_csv(data_path,  sep=',', encoding='utf-8')

def main():
	data_path = '/data/sf_dummy_data.csv'
	df = get_data(data_path)
	aggregation = get_aggregation()
	df['sales'] = df.volume * df.unit_price
	df = df[aggregation.extend(['sales'])]
	df = df.groupby(aggregation).sum()
	save_adjustment(df)
	print(df)


if __name__ == "__main__":

    main()