import json
from flask import Flask, render_template, request, redirect, jsonify, url_for
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from webapp import app
from webapp import query
import time
#DBSession = sessionmaker(bind=engine)
#session = DBSession()

@app.route('/')
@app.route('/evaluations')
def index():
    return render_template('index.html')


@app.route('/evaluations/search_best_models', methods=['POST'])
def search_best_models():
    if request.method == 'POST':
        metric = request.form['metric']
        timestamp = request.form['timestamp']
        if len(request.form['parameter']) == 0:
            parameter = None
        else:
            parameter = request.form['parameter']

        if len(request.form['number']) == 0:
            number = 15
        else:
            number = request.form['number']

    output = query.get_best_models(timestamp=timestamp, metric=metric, parameter=parameter, number=number)
    #print(output)
    #return render_template('index.html',tables=[output.to_html(classes='bestmodels')], number=number, parameter=parameter)
    try:
        output = output.to_dict('records')
        return jsonify(results=(output))
    except:
        print('there are some problems')
        return jsonify({"sorry": "Sorry, no results! Please try again."}), 500

@app.route('/evaluations/<int:model_id>/prediction',methods=['GET','POST'])
def get_model_prediction(model_id):
    tic = time.time()
    output = query.get_model_prediction(id=model_id)
    print("Query Time: ", time.time() - tic)
    return render_template('prediction.html',tables=[output.to_html(classes='bestmodels')])
    #output.to_dict('records')
    #return jsonify(results=(output))
    #return render_template('individual.html')

@app.route('/evaluations/within_model',methods=['GET','POST'])
def within_model():
    return render_template('within_model.html')

@app.route('/evaluations/between_models',methods=['GET','POST'])
def between_models():
    return render_template('between_models.html')


