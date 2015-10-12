import pickle
import glob
from os import path
import dateutil.parser
from collections import namedtuple
import sys
from threading import Lock

from flask import abort

from webapp.evaluation import precision_at_x_percent
from webapp import config


cache = {}
cache_lock = Lock()


def timestamp_from_path(pkl_path):
    prefix = path.join(config.results_folder, "police_eis_results_")
    ts = pkl_path.replace(prefix, "")
    ts = ts.replace(".pkl", "")
    return ts


Experiment = namedtuple("Experiment", ["timestamp", "config", "score", "data"])


def experiment_summary(pkl_file):
    data = read_pickle(pkl_file)
    model_config = data["config"]
    if "parameters" not in model_config:
        model_config["parameters"] = "?"

    if "train_start_date" not in data:
        model_config["train_start_date"] = "01Jan1970"
    else:
        model_config["train_start_date"] = data["train_start_date"].strftime("%d%b%Y")

    model_config["test_end_date"] = data["test_end_date"].strftime("%d%b%Y")


    # model_config["features"] = data["features"]
    model_config["feature_summary"] = feature_summary(model_config["features"])
    prec_at = precision_at_x_percent(
        data["test_labels"], data["test_predictions"],
        x_percent=0.01)
    return Experiment(dateutil.parser.parse(timestamp_from_path(pkl_file)),
                      model_config,
                      prec_at,
                      data)


def update_experiments_cache():
    experiments = glob.glob(config.results_folder)
    experiments = glob.glob(path.join(config.results_folder, "*.pkl"))
    with cache_lock:
        for pkl in experiments:
            ts = timestamp_from_path(pkl)
            if ts not in cache:
                cache[ts] = experiment_summary(pkl)
    # todo delete experiments that were remove from cache


def read_pickle(pkl_file):
    with open(pkl_file, "rb") as f:
        if sys.version_info < (3, 0):
            content = pickle.load(f)
        else:
            content = pickle.load(f, encoding='latin1')
    return content


def get_labels_predictions(timestamp):
    update_experiments_cache()
    # risk of dirty reads here because outside of lock
    if timestamp not in cache:
        abort(404)
    exp = cache[timestamp]
    return exp.data["test_labels"], exp.data["test_predictions"]


def get_feature_importances(timestamp):
    update_experiments_cache()
    # risk of dirty reads here because outside of lock
    if timestamp not in cache:
        abort(404)
    exp = cache[timestamp]
    return exp.data["features"], exp.data["feature_importances"]


def get_experiments_list():
    update_experiments_cache()
    # risk of dirty reads here because outside of lock
    experiments_copy = [Experiment(e.timestamp, e.config,
                                   e.score, None) for e in cache.values()]
    return experiments_copy


def feature_summary(features):
    known_features = ['height_weight', 'ia_history', 'education', 'daysexperience',
                      'yearsexperience', 'malefemale']
    used_features = [key for key, val in features.items() if val == True]

    not_used = set(known_features) - set(used_features)
    if len(not_used) == 0:
        return "all"
    else:
        not_used = [n for n in not_used if not n.startswith("imputation_")]
        # any_census_used = any([True for n in features
        #                       if n.startswith("rate_")])
        # if not any_census_used:
        #     not_used = [n for n in not_used if not n.startswith("rate_")]
        #     not_used.append("census")
        return "not used: {}".format(", ".join(not_used))

# will be run when webapp first starts
print("Initializing experiments list")
update_experiments_cache()
print("...finished")
