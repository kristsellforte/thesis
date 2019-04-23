from datetime import datetime
import pandas as pd
from fbprophet import Prophet

def get_data(data_path):
    try:
        with open(data_path, 'r') as f:
            return pd.read_csv(f, delimiter=';')
    except FileNotFoundError:
        return {}


def transform_data(df):
    df['ds'] = df['date']
    df['y'] = df['sales']
    df = df[['ds', 'y', 'item_id']]

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


def save_model(model, scores, item_id):
    version_number = int(max(scores.keys())) + 1
    pickle_path = '/models/linear_regression/' + str(version_number) + '_' + str(item_id) + '.pkl'
    pickle.dump(model, open(pickle_path, 'wb'))


def main():
    scores_path = '/scores/prophet.json'
    data_path = '/data/train.csv'
    df = get_data(data_path)
    df = transform_data(df)
    for item_id in df.item_id.unique():
        df_item = df[df['item_id'] == item_id]
        model = train_model(df)
        scores = get_scores(scores_path)
        save_model(model, scores, item_id)
    

if __name__ == "__main__":

    main()
