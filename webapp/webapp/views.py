import json
from flask import Flask, render_template, request, redirect, jsonify, url_for
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from webapp import app
from webapp import query

#DBSession = sessionmaker(bind=engine)
#session = DBSession()

@app.route('/')
@app.route('/evaluations')
def index():
    return render_template('index.html')


@app.route('/evaluations/search_best_models', methods=['GET', 'POST'])
def search_best_models():
    if request.method == 'POST':
        metric = request.form['metric']
        if len(request.form['parameter']) == 0:
            parameter = None
        else:
            parameter = request.form['parameter']

        if len(request.form['number']) == 0:
            number = 10
        else:
            number = request.form['number']

    timestamp = '2016-08-03'
    output = query.get_best_models(timestamp=timestamp, metric=metric, parameter=parameter, number=number)
    return render_template('index.html',tables=[output.to_html(classes='bestmodels')], number=number, parameter=parameter)
