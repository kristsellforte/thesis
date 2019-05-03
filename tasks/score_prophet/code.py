from fbprophet.diagnostics import cross_validation, performance_metrics
import json
import pickle


def cross_validate(model):
    df_cv = cross_validation(model, initial='270 days', period='90 days', horizon = '30 days')
    return df_cv


def get_performance_metrics(df_cv):
    df_p = performance_metrics(df_cv)
    return df_p


def get_scores(scores_path):
    try:
        with open(scores_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return { '0': {} }


def get_version_number(scores):
    return str(max(scores.keys()))


def get_model(model_path):
    model = pickle.load(open(model_path, 'rb'))

    return model

def save_score(current_score, scores_path):
    with open(scores_path, 'r+') as scores_json:
        scores = json.load(scores_json)
        version_number = get_version_number(scores)
        scores_json.seek(0)
        scores[version_number] = current_score
        json.dump(scores, scores_json, indent=4)


def main():
    scores_path = '/scores/prophet.json'
    scores = get_scores(scores_path)
    version_number = get_version_number(scores)
    current_scores = scores[version_number]
    if current_scores.keys():
        keys = current_scores.keys()
        for key in keys:
            model_path = '/models/prophet/' + version_number + '/' + key
            for child_key in current_scores[key].keys():
                model_path = model_path + '_' + child_key + '.pkl'
                model = get_model(model_path)
                df_cv = cross_validate(model)
                df_p = get_performance_metrics(df_cv)
                current_scores[key][child_key] = df_p.to_json(orient='records')
                # print(df_p.t
    else:
        model_path = '/models/prophet/' + version_number + '.pkl'
        model = get_model(model_path)
        df_cv = cross_validate(model)
        df_p = performance_metrics(df_cv)
        current_scores = df_p.to_json(orient='records')

    save_score(current_scores, scores_path)


if __name__ == "__main__" :

    main()


