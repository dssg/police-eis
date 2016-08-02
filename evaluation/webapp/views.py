#!/usr/bin/python3
from io import BytesIO
from itertools import groupby
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt

from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from flask import make_response, render_template, abort

from webapp import app
from webapp.evaluation import *
from webapp.ioutils import *
from webapp import config
import pdb


class SortedDisplayDict(dict):
   def __str__(self):
       return "{" + ", ".join("%r: %r" % (key, self[key]) for key in sorted(self)) + "}"


@app.route('/')
def index():
    experiments = get_experiments_list()

    # group by date, newest first
    experiments = sorted(experiments, key=lambda r: r.timestamp.date(),
                         reverse=True)
    experiments = [(date, list(items)) for date, items in groupby(
        experiments, lambda r: r.timestamp.date())]

    # for each date sort its results, best first
    experiments = [(date, sorted(items, key=lambda r: r.score, reverse=True))
                   for date, items in experiments]

    return render_template('overview.html', experiments=experiments)


@app.route('/<timestamp>')
def details(timestamp):
    # will fail with 404 if exp not known
    get_labels_predictions(timestamp)
    groups = get_aggregate_scores(timestamp)
    #groups["division_order"] = sorted(groups["divisions"].keys())
    #groups["unit_order"] = sorted(groups["units"].keys())
    eis_baseline, fpr, tpr, fnr, tnr, threshold_levels, config = get_baselines(timestamp)
    fpr_dict = SortedDisplayDict(fpr)
    tpr_dict = SortedDisplayDict(tpr)
    fnr_dict = SortedDisplayDict(fnr)
    tnr_dict = SortedDisplayDict(tnr)
    return render_template('details.html', timestamp=timestamp, groups=groups,
                           eis_baseline=eis_baseline, fpr=fpr_dict, tpr=tpr_dict,
                           fnr=fnr_dict, tnr=tnr_dict,
                           config=config, threshold_levels=threshold_levels)


@app.route("/<timestamp>/norm_confusions")
def normalized_confusion_matrix(timestamp):
    test_labels, test_predictions = get_labels_predictions(timestamp)
    matrix_fig = plot_normalized_confusion_matrix(test_labels,
                                                  test_predictions)
    return serve_matplotlib_fig(matrix_fig)


@app.route("/<timestamp>/confusion_at_x")
def confusion_at_x(timestamp):
    test_labels, test_predictions = get_labels_predictions(timestamp)
    new_matrix_fig = plot_confusion_matrix_at_x_percent(test_labels,
                                                  test_predictions, 0.10)
    return serve_matplotlib_fig(new_matrix_fig)


@app.route("/<timestamp>/top_normalized_confusion_at_x")
def top_normalized_confusion_at_x(timestamp):
    test_labels, test_predictions = get_labels_predictions(timestamp)
    new_matrix_fig = plot_normalized_confusion_matrix_at_x_percent(test_labels,
                                                  test_predictions, 0.01)
    return serve_matplotlib_fig(new_matrix_fig)


@app.route("/<timestamp>/top_confusion_at_x")
def top_confusion_at_x(timestamp):
    test_labels, test_predictions = get_labels_predictions(timestamp)
    new_matrix_fig = plot_confusion_matrix_at_x_percent(test_labels,
                                                  test_predictions, 0.01)
    return serve_matplotlib_fig(new_matrix_fig)


@app.route("/<timestamp>/normalized_confusion_at_x")
def normalized_confusion_at_x(timestamp):
    test_labels, test_predictions = get_labels_predictions(timestamp)
    new_matrix_fig = plot_normalized_confusion_matrix_at_x_percent(test_labels,
                                                  test_predictions, 0.10)
    return serve_matplotlib_fig(new_matrix_fig)


@app.route("/<timestamp>/improvement_over_baseline")
def improvement_over_baseline(timestamp):
    eis_baseline, fpr, tpr, fnr, tnr, threshold_levels, config = get_baselines(timestamp)
    improve_fig = plot_fp_tp_percent(eis_baseline, fpr, tpr, threshold_levels)
    return serve_matplotlib_fig(improve_fig)


@app.route("/<timestamp>/improvement_over_baseline_abs")
def improvement_over_baseline_abs(timestamp):
    eis_baseline, fpr, tpr, fnr, tnr, threshold_levels, config = get_baselines(timestamp)
    improve_fig = plot_fp_tp_absolute(eis_baseline, fpr, tpr, threshold_levels)
    return serve_matplotlib_fig(improve_fig)


@app.route("/<timestamp>/improvement_over_baseline_abs_nothresh")
def improvement_over_baseline_abs_nothresh(timestamp):
    eis_baseline, fpr, tpr, fnr, tnr, threshold_levels, config = get_baselines(timestamp)
    improve_fig = plot_fp_tp_absolute_nothresh(eis_baseline, fpr, tpr, threshold_levels)
    return serve_matplotlib_fig(improve_fig)


@app.route("/<timestamp>/improvement_over_baseline_nothresh")
def improvement_over_baseline_nothresh(timestamp):
    eis_baseline, fpr, tpr, fnr, tnr, threshold_levels, config = get_baselines(timestamp)
    improve_fig = plot_fp_tp_percent_nothresh(eis_baseline, fpr, tpr, threshold_levels)
    return serve_matplotlib_fig(improve_fig)


@app.route("/<timestamp>/importances")
def feature_importances(timestamp):
    features, importances = get_feature_importances(timestamp)
    importance_fig = plot_feature_importances(features, importances)
    return serve_matplotlib_fig(importance_fig)


@app.route("/<timestamp>/precision-recall")
def precision_recall(timestamp):
    test_labels, test_predictions = get_labels_predictions(timestamp)
    prec_recall_fig = plot_precision_recall_n(test_labels, test_predictions)
    return serve_matplotlib_fig(prec_recall_fig)


@app.route("/<timestamp>/precision-cutoff")
def precision_cutoff(timestamp):
    test_labels, test_predictions = get_labels_predictions(timestamp)
    prec_cutoff_fig = plot_precision_cutoff(test_labels, test_predictions)
    return serve_matplotlib_fig(prec_cutoff_fig)


@app.route("/<timestamp>/ROC")
def ROC(timestamp):
    test_labels, test_predictions = get_labels_predictions(timestamp)
    roc_fig = plot_ROC(test_labels, test_predictions)
    return serve_matplotlib_fig(roc_fig)


@app.route("/growth")
def growth():
    experiments = get_experiments_list()

    # group by date, newest first
    experiments = sorted(experiments, key=lambda r: r.timestamp.date(),
                         reverse=True)
    experiments = [(date, list(items)) for date, items in groupby(
                   experiments, lambda r: r.timestamp.date())]

    # only keep best result for each day
    experiments = [(date, sorted(items, key=lambda r: r.score,
                   reverse=True)[0]) for date, items in experiments]
    experiments = [(date, best.score) for date, best in experiments]

    growth_fig = plot_growth(experiments)
    return serve_matplotlib_fig(growth_fig)


def serve_matplotlib_fig(fig):
    canvas = FigureCanvas(fig)
    png_output = BytesIO()
    canvas.print_png(png_output)
    with app.app_context():
        response = make_response(png_output.getvalue())
        response.headers['Content-Type'] = 'image/png'
    return response
