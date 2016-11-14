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


@app.route('/evaluations/search_models', methods=['POST'])
def search_models():
    f = request.form
    query_arg = {}
    metric =[]
    parameter=[]
    for key in f.keys():
        if 'parameter' in key:
            parameter.append(key)
        elif 'metric' in key:
            metric.append(key)
    mp = [f[m]+'@'+str(float(f[p])) for m, p in zip(sorted(metric),sorted(parameter))]
    #query_arg['number'] = f['number']
    query_arg['timestamp'] = f['timestamp']
    query_arg['metric'] = mp

    output = query.get_models(query_arg)
    #print(output)
    try :
        output = output.to_dict('records')
        #print(output)
        return jsonify(results=(output))
        #return render_template('index.html',tables=[output.to_html(classes='bestmodels')])
    except:
        print('there are some problems')
        return jsonify({"sorry": "Sorry, no results! Please try again."}), 500
    #dict(f).keys()


@app.route('/evaluations/search_best_models', methods=['POST'])
def search_best_models():
    if request.method == 'POST':
        f = request.form
        metric = f['metric']
        timestamp = f['timestamp']

        if len(f['parameter']) == 0:
            parameter = None
        else:
            parameter = f['parameter']

        if len(f['number']) == 0:
            number = 15
        else:
            number = request.form['number']
        timestamp = request.form['timestamp']
    output = query.get_best_models(timestamp=timestamp, metric=metric, parameter=parameter, number=number)
    #print(output)
    #return render_template('index.html',tables=[output.to_html(classes='bestmodels')], number=number, parameter=parameter)
    try:
        output = output.to_dict('records')
        return jsonify(results=(output))
    except:
        print('there are some problems')
        return jsonify({"sorry": "Sorry, no results! Please try again."}), 500

@app.route('/evaluations/<int:model_id>/model',methods=['GET','POST'])
def get_model_prediction(model_id):
    tic = time.time()
    output = query.get_model_prediction(id=model_id)
    print("get_model_prediction")
    print("Query Time: ", time.time() - tic)
    return render_template('model.html',tables=[output.to_html(classes='bestmodels')])

    #output.to_dict('records')
    #return jsonify(results=(output))
    #return render_template('individual.html')

@app.route('/evaluations/<int:model_id>/model_result',methods=['GET','POST'])
def get_model_result(model_id):
    output = query.get_model_prediction(id=model_id)
    try:
        output = output.to_dict('records')
        return jsonify(results=(output))
    except:
        print('there are some problems')
        return jsonify({"sorry": "Sorry, no results! Please try again."}), 500

@app.route('/evaluations/within_model',methods=['GET','POST'])
def within_model():
    return render_template('within_model.html')

@app.route('/evaluations/between_models',methods=['GET','POST'])
def between_models():
    return render_template('between_models.html')


