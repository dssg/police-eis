import sys
import os
import pandas as pd
import subprocess
import argparse
import pdb
import pickle
from eis import setup_environment

"""
Code to take top performing recent models and
put them in the evaluation webapp for further
examination.

Examples:
--------

python prepare.py '2016-08-03' 'auc'
python prepare.py '2016-08-03' 'recall@' -p '0.01'
python prepare.py '2016-08-03' 'precision@' -p '10.0' -n 10
python prepare.py '2016-08-03' 'precision@' -p '10.0' -n 10 -d 'example_directory/'


"""

engine = setup_environment.get_database()
try:
    con = engine.raw_connection()
    con.cursor().execute("SET SCHEMA '{}'".format('models'))
except:
    pass


def get_metric_best_models(timestamp, metric, parameter=None, number=25):

    """
    --------------------------------------------------------
    Get the EVALUATION METRIC VALUE of the best models
    by the specified timestamp and given metric
    --------------------------------------------------------
    ARGUMENTS:
        timestamp:  models run on or after given timestamp
                    example: '2016-08-03'
        metric:     metric to be optimized
                    example: 'precision@'
        parameter:  parameter value or threshold if any
                    default=None
                    example: '10.0'
        number:     maximum number of desired results
                    default = 25
    --------------------------------------------------------
    """

    if parameter is None:
        query = ("      SELECT value FROM results.evaluations JOIN results.models \
                        ON evaluations.model_id=models.model_id \
                        WHERE run_time >= '{}' \
                        AND value is not null \
                        AND metric = '{}' \
                        ORDER BY value DESC LIMIT {} ; ").format(timestamp, metric, number)


    elif parameter is not None:
        query = ("      SELECT value FROM results.evaluations JOIN results.models \
                        ON evaluations.model_id=models.model_id \
                        WHERE run_time >= '{}' \
                        AND value is not null \
                        AND metric = '{}' \
                        AND parameter = '{}' \
                        ORDER BY value DESC LIMIT {} ; ").format(timestamp, metric, parameter, number)



    df_models = pd.read_sql(query, con=con)
    output = df_models["value"].apply(lambda x: str(x)).values
    statement = "Resulting metric for models with best {} run on or after {}: \n".format(metric, timestamp)
    print (statement, output)
    return output


def get_best_models_id(timestamp, metric, parameter=None, number=25):

    """
    --------------------------------------------------------
    Get the MODEL ID of the best models
    by the specified timestamp and given metric
    --------------------------------------------------------
    ARGUMENTS:
        timestamp:  models run on or after given timestamp
                    example: '2016-08-03'
        metric:     metric to be optimized
                    example: 'precision@'
        parameter:  parameter value or threshold if any
                    default=None
                    example: '10.0'
        number:     maximum number of desired results
                    default = 25
    --------------------------------------------------------
    """

    if parameter is None:
        query = ("      SELECT run_time FROM results.evaluations JOIN results.models \
                        ON evaluations.model_id=models.model_id \
                        WHERE run_time >= '{}' \
                        AND value is not null \
                        AND metric = '{}' \
                        ORDER BY value DESC LIMIT {} ; ").format(timestamp, metric, number)


    elif parameter is not None:
        query = ("      SELECT run_time FROM results.evaluations JOIN results.models \
                        ON evaluations.model_id=models.model_id \
                        WHERE run_time >= '{}' \
                        AND value is not null \
                        AND metric = '{}' \
                        AND parameter = '{}' \
                        ORDER BY value DESC LIMIT {} ; ").format(timestamp, metric, parameter, number)


    df_models = pd.read_sql(query, con=con)
    output = df_models['run_time'].apply(lambda x: str(x).replace(' ', 'T')).values
    print(output)
    return output



def get_best_models(timestamp, metric, parameter=None, number=25):

    """
    --------------------------------------------------------
    Get the REPORT of the best models
    by the specified timestamp and given metric

    RETURNS RUN TIME, MODEL TYPE, METRIC, and VALUE
    OR
    RUN TIME, MODEL TYPE, METRIC, PARAMETER, and VALUE
    --------------------------------------------------------
    ARGUMENTS:
        timestamp:  models run on or after given timestamp
                    example: '2016-08-03'
        metric:     metric to be optimized
                    example: 'precision@'
        parameter:  parameter value or threshold if any
                    default=None
                    example: '10.0'
        number:     maximum number of desired results
                    default = 25
    --------------------------------------------------------
    """

    if parameter is None:
        query = ("      SELECT run_time, model_type, metric, value FROM results.evaluations JOIN results.models \
                        ON evaluations.model_id=models.model_id \
                        WHERE run_time >= '{}' \
                        AND value is not null \
                        AND metric = '{}' \
                        ORDER BY value DESC LIMIT {} ; ").format(timestamp, metric, number)


    elif parameter is not None:
        query = ("      SELECT run_time, model_type, metric, parameter, value FROM results.evaluations JOIN results.models \
                        ON evaluations.model_id=models.model_id \
                        WHERE run_time >= '{}' \
                        AND value is not null \
                        AND metric = '{}' \
                        AND parameter = '{}' \
                        ORDER BY value DESC LIMIT {} ; ").format(timestamp, metric, parameter, number)



    df_models = pd.read_sql(query, con=con)
    output = df_models
    statement = "Resulting top models with best {} run on or after {}: \n".format(metric, timestamp)
    print (statement, output)
    return output


def get_pickle_best_models(timestamp, metric, parameter=None, number=25, directory="results/"):

    """
    --------------------------------------------------------
    Get the PICKLE FILE of the best models
    by the specified timestamp and given metric

    RETURNS the PICKLE FILE to a DIRECTORY
    --------------------------------------------------------
    ARGUMENTS:
        timestamp:  models run on or after given timestamp
                    example: '2016-08-03'
        metric:     metric to be optimized
                    example: 'precision@'
        parameter:  parameter value or threshold if any
                    default=None
                    example: '10.0'
        number:     maximum number of desired results
                    default = 25
    --------------------------------------------------------
    """

    if parameter is None:
        query = ("SELECT pickle_blob, run_time  FROM \
                    (SELECT evaluations.model_id, run_time \
                        FROM results.evaluations JOIN results.models \
                        ON evaluations.model_id=models.model_id \
                        WHERE run_time >= '{}' \
                        AND value is not null \
                        AND metric = '{}' \
                        ORDER BY value DESC LIMIT {}) \
                    AS top_models \
                    INNER JOIN results.data \
                    ON top_models.model_id=data.model_id ; " ).format(timestamp, metric, number)

    elif parameter is not None:
        query = ("SELECT pickle_blob, run_time  FROM \
                    (SELECT evaluations.model_id, run_time \
                        FROM results.evaluations JOIN results.models \
                        ON evaluations.model_id=models.model_id \
                        WHERE run_time >= '{}' \
                        AND value is not null \
                        AND metric = '{}' \
                        AND parameter = '{}' \
                        ORDER BY value DESC LIMIT {}) \
                    AS top_models \
                    INNER JOIN results.data \
                    ON top_models.model_id=data.model_id ; " ).format(timestamp, metric, parameter, number)



    df_models = pd.read_sql(query, con=con)
    N = len(df_models['pickle_blob'])

    for file_number in range(0, N):
        pickle_file = pickle.loads(df_models['pickle_blob'].iloc[file_number])
        file_name = df_models['run_time'].apply(lambda x: str(x).replace(' ', 'T')).iloc[file_number]
        if parameter is None:
            full_file_name = "police_eis_results_"+"top_"+metric+"any"+"_"+file_name+".pkl"
        elif parameter is not None:
            full_file_name = "police_eis_results_"+"top_"+metric+parameter+"_"+file_name+".pkl"
        file_path = directory+full_file_name
        pickle.dump(pickle_file, open( file_path, "wb" ) )

    return None


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("timestamp", type=str, help="show models more recent than a given timestamp")
    parser.add_argument("metric", type=str, help="specify a desired metric to optimize")
    parser.add_argument("-p", "--parameter", default=None, type=str, help="specify a desired parameter or threshold for your metric, default=None")
    parser.add_argument("-n", "--number", default=25, type=int, help="maximum number of results to return, default=25")
    parser.add_argument("-d", "--directory", default="results/", type=str, help="file directory for pickle files, default='results/'")
    args = parser.parse_args()

    print("[*] Updating model list...")
    models = get_best_models(args.timestamp, args.metric, args.parameter, args.number)
    print("[*] Dumping requested pickle files to results...")
    pickles = get_pickle_best_models(args.timestamp, args.metric, args.parameter, args.number, args.directory)
    print("[*] Done!")
