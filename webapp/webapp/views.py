import json
from flask import Flask, render_template, request, redirect, jsonify, url_for
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from webapp import app


CONFIG_PATH = "/home/tlin/default_profile.json"
with open(CONFIG_PATH) as f:
    config = json.load(f)

engine = create_engine('postgres://', connect_args=config)
#DBSession = sessionmaker(bind=engine)
#session = DBSession()

@app.route('/')
@app.route('/evaluations')
def index():
    return render_template('index.html')

@app.route('/evaluations/search_ids')
def search_ids():
    timestamp = '2016-08-03'
    metric = 'auc'
    number = 5
    query = ("      SELECT run_time FROM results.evaluations JOIN results.models \
                    ON evaluations.model_id=models.model_id \
                    WHERE run_time >= '{}' \
                    AND value is not null \
                    AND metric = '{}' \
                    ORDER BY value DESC LIMIT {} ; ").format(timestamp, metric, number)
    df_models = pd.read_sql(query, con=engine)
    print(df_models)
    #output = df_models['run_time'].apply(lambda x: str(x).replace(' ', 'T')).values
    output = df_models
    return render_template('index.html',tables=[output.to_html(classes='bestids')])

@app.route('/evaluations/search_best_models', methods=['GET', 'POST'])
def search_best_models():
    if request.method == 'POST':
        metric = request.form['metric'] + "@"
        parameter = float(request.form['parameter'])
        number = request.form['number']

    timestamp = '2016-08-03'

    query = ("      SELECT run_time, model_type, metric, parameter, value FROM results.evaluations JOIN results.models \
                    ON evaluations.model_id=models.model_id \
                    WHERE run_time >= '{}' \
                    AND value is not null \
                    AND metric = '{}' \
                    AND parameter = '{}' \
                    ORDER BY value DESC LIMIT {} ; ").format(timestamp, metric, parameter, number)
    df_models = pd.read_sql(query, con=engine)
    output = df_models
    return render_template('index.html',tables=[output.to_html(classes='bestmodels')], number=number, parameter=parameter)
