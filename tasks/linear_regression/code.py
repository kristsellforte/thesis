import pandas as pd
from sklearn import linear_model
import statsmodels.api as sm
import json
import pickle
from datetime import datetime


def get_data(data_path):
    try:
        with open(data_path, 'r') as f:
            return pd.read_csv(f)
    except FileNotFoundError:
        return {}


def prepare_data(df):
    df['date'] = df['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').toordinal())
    x = df[['date', 'store', 'item']]
    y = df[['sales']]

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
    pickle_path = './models/linear_regression/' + str(version_number) + '.pkl'

    pickle.dump(model, open(pickle_path, 'wb'))


def main():
    scores_path = './scores.json'
    data_path = './data.csv'
    df = get_data(data_path)
    x, y = prepare_data(df)
    model = linear_regression_sklearn(x, y)
    scores = get_scores(scores_path)
    save_model(model, scores)


if __name__ == "__main__" :

    main()

# date = datetime.strptime('2018-12-31', '%Y-%m-%d').toordinal()
# store = 10
# item = 50
# print ('Predicted sales: \n', regr.predict([[date, store, item]]))