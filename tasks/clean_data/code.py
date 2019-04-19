import pandas as pd
import csv

def get_data(data_path):
    try:
        with open(data_path, 'r') as f:
            return pd.read_csv(f, delimiter=';')
    except FileNotFoundError:
        return {}


def save_result(df, data_path):
	df.to_csv(data_path, sep='\t', encoding='utf-8')


def main():
	data_path = '/data/train.csv'
	df = get_data(data_path)
    df['sales'] = df.volume * df.unit_price
	save_result(df, data_path)


if __name__ == "__main__":

    main()