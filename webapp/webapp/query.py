import pandas as pd
from sqlalchemy import create_engine
import json
from webapp import app
import os

with open(os.path.join(app.instance_path, 'default_profile.json')) as f:
    config = json.load(f)

dbengine = create_engine('postgres://', connect_args=config)

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
        query = ("SELECT run_time, model_type, metric, parameter, value FROM results.evaluations JOIN results.models "
                 "ON evaluations.model_id=models.model_id "
                 "WHERE run_time >= '{}' "
                 "AND value is not null "
                 "AND metric = '{}' "
                 "ORDER BY value DESC LIMIT {} ").format(timestamp, metric, number)

    elif parameter is not None:
        parameter = float(parameter)
        metric = metric + "@"
        query = ("SELECT run_time, model_type, metric, parameter, value FROM results.evaluations JOIN results.models "
                 "ON evaluations.model_id=models.model_id "
                 "WHERE run_time >= '{}' "
                 "AND value is not null "
                 "AND metric = '{}' "
                 "AND parameter = '{}' "
                 "ORDER BY value DESC LIMIT {} ; ").format(timestamp, metric, parameter, number)

    df_models = pd.read_sql(query, con=dbengine)
    output = df_models
    return output
