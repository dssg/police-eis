import numpy as np
import numpy as np
import pandas as pd
import logging
import sys
import pickle
import pdb
import datetime

from eis import dataset, setup_environment

log = logging.getLogger(__name__)

try:
    engine, config = setup_environment.get_database()
    con = engine.raw_connection()
    con.cursor().execute("SET SCHEMA '{}'".format(config['schema']))
except:
    pass


def aggregate(ids, predictions, fake_today):
    """
    Take officer level predictions and produce risk scores for 
    divisons and units.
    """
    
    unitpreds = group_predictions("units", ids, predictions, fake_today)
    divpreds = group_predictions("divisions", ids, predictions, fake_today)

    return {"units": unitpreds,
            "divisions": divpreds}


def make_aggregate_query(group_type, fake_today):
    group_columns = {"divisions": "div",
                     "units": "unityp"}
    query_group = ("select distinct f.newid, f.{group} from {table} AS f "
                   "join "
                   "(select g.newid, max(g.date_ln) as maxdate from {table} g "
                   "where date_ln <= '{date}'::date group by g.newid) AS g "
                   "on f.newid=g.newid and f.date_ln=maxdate").format(
                       group=group_columns[group_type], table=config["logonoff"],
                       date=fake_today)
    return query_group


def group_predictions(group_type, ids, predictions, fake_today):
    query = make_aggregate_query(group_type, fake_today)
    df = pd.read_sql(query, con=con)
    predf = pd.DataFrame({"newid": list(ids), "predictions": predictions})

    labelled_df = pd.merge(predf, df, on="newid", how="left")
    labelled_df = labelled_df.dropna()

    column_name = list(df.columns)
    column_name.remove('newid')

    group_pred = dict.fromkeys(config[group_type])
    for each_group in config[group_type]:
        try:
            officers_in_group = labelled_df[labelled_df[column_name] == each_group]
            group_pred[each_group] = {"number": len(officers_in_group),
                                      "risk": np.mean(officers_in_group["predictions"])}
        except:
            group_pred[each_group] = {"number": 0,
                                      "risk": 0}

    return group_pred