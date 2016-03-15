import sys
import os
import pandas as pd
import subprocess

from eis import setup_environment


engine, config = setup_environment.get_database()
try:
    con = engine.raw_connection()
    con.cursor().execute("SET SCHEMA '{}'".format('models'))
except:
    pass


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
    print("[*] Updating model list...")
    ids = get_best_models()
    raw_outputs_dir = '/mnt/data4/jhelsby/newpilot/'
    webapp_display_dir = '/mnt/data4/jhelsby/currentdisplay/'
    prepare_webapp_display(ids, raw_outputs_dir, webapp_display_dir)   
