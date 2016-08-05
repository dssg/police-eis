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


"""

engine, config = setup_environment.get_database()
try:
    con = engine.raw_connection()
    con.cursor().execute("SET SCHEMA '{}'".format('models'))
except:
    pass


def get_metric_best_models(timestamp, metric, parameter=None, number=25):

    """
    Get the evaluation results of the best recent models
    by the specified timestamp and given metric
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


def get_best_recent_models(timestamp, metric):
    """
    Get the model id of the best recent models
    by the specified timestamp and given metric
    """

    query   =  ("   SELECT run_time FROM results.evaluations JOIN results.models \
                    ON evaluations.model_id=models.model_id \
                    WHERE run_time >= '{}' \
                    AND {} is not null \
                    ORDER BY {} DESC LIMIT 25; ").format(timestamp, metric, metric)

    df_models = pd.read_sql(query, con=con)
    output = df_models['run_time'].apply(lambda x: str(x).replace(' ', 'T')).values
    return output


def get_best_models(metric):
    """
    Grab the identifiers of the best performing (top AUC) models
    from the database.
    """
    #query = "SELECT id_timestamp FROM models.full ORDER BY auc DESC LIMIT 25"
    query   =  ("   SELECT run_time FROM results.evaluations JOIN results.models \
                    ON evaluations.model_id=models.model_id \
                    WHERE {} is not null \
                    ORDER BY {} DESC LIMIT 25; ").format(metric, metric)

    df_models = pd.read_sql(query, con=con)
    output = df_models['run_time'].apply(lambda x: str(x).replace(' ', 'T')).values
    return output


def get_pickle_best_models(timestamp, metric, parameter=None, number=25):

    """
    Get the pickle file of the best recent models
    by the specified timestamp and given metric

    Dumps top pickle files from database to results directory.
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
        file_path = "results/"+full_file_name
        pickle.dump(pickle_file, open( file_path, "wb" ) )

    return None


def prepare_webapp_display(ids, src_dir, dest_dir):
    """
    Move the relevant webapp files into the directory that the evaluation
    webapp pulls from.
    """
    for model in ids:
        filename = '{}police_eis_results_{}.pkl'.format(src_dir, model)
        subprocess.check_output(["cp", filename, dest_dir])
    return None


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("timestamp", type=str, help="show models more recent than a given timestamp")
    parser.add_argument("metric", type=str, help="specify a desired metric to optimize")
    parser.add_argument("-p", "--parameter", default=None, type=str, help="specify a desired parameter or threshold for your metric, default=None")
    parser.add_argument("-n", "--number", default=25, type=int, help="maximum number of results to return, default=25")
    args = parser.parse_args()

    print("[*] Updating model list...")
    metrics = get_metric_best_models(args.timestamp, args.metric, args.parameter, args.number)
    print("[*] Dumping requested pickle files to results...")
    pickles = get_pickle_best_models(args.timestamp, args.metric, args.parameter, args.number)
    print("[*] Done!")
