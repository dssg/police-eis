import sys
import os
import pandas as pd
import subprocess
import argparse

sys.path.append("../")

from eis import setup_environment
#from setup_environment import *

"""
Code to take top performing recent models and
put them in the evaluation webapp for further
examination.

Example:
--------
python prepare.py '2016-07-28' auc_score
"""

engine, config = setup_environment.get_database()
try:
    con = engine.raw_connection()
    con.cursor().execute("SET SCHEMA '{}'".format('models'))
except:
    pass



def get_best_recent_models(timestamp, metric):
    """
    Grab the identifiers of the best recent models by specified metric
    """

    query   =  ("   SELECT run_time FROM results.evaluations JOIN results.models \
                    ON evaluations.model_id=models.model_id \
                    WHERE run_time >= '{}' \
                    AND {} is not null \
                    ORDER BY {} DESC LIMIT 25; ").format(timestamp, metric, metric)

    df_models = pd.read_sql(query, con=con)
    output = df_models['run_time'].apply(lambda x: str(x).replace(' ', 'T')).values
    print (output)
    return output




def get_best_models():
    """
    Grab the identifiers of the best performing (top AUC) models
    from the database.
    """
    query = "SELECT id_timestamp FROM models.full ORDER BY auc DESC LIMIT 25"
    df_models = pd.read_sql(query, con=con)
    return df_models['id_timestamp'].apply(lambda x: str(x).replace(' ', 'T')).values


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
    args = parser.parse_args()

    print("[*] Updating model list...")
    ids = get_best_recent_models(args.timestamp, args.metric)
    raw_outputs_dir = '/mnt/data4/jhelsby/newpilot/'
    webapp_display_dir = '/mnt/data4/jhelsby/currentdisplay/'
    prepare_webapp_display(ids, raw_outputs_dir, webapp_display_dir)
