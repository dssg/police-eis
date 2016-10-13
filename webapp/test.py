import json
from sqlalchemy import create_engine
import pandas as pd

CONFIG_PATH = "/home/tlin/default_profile.json"
with open(CONFIG_PATH) as f:
    config = json.load(f)

engine = create_engine('postgres://', connect_args=config)

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


    df_models = pd.read_sql(query, con=engine)
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

    df_models = pd.read_sql(query, con=engine)
    output = df_models
    statement = "Resulting top models with best {} run on or after {}: \n".format(metric, timestamp)
    print (statement, output)
    return output

timestamp = '2016-08-03'
metric = 'precision@'
parameter = 10.0
number = 5
get_best_models(timestamp, metric, parameter=parameter, number=number)

'''
    timestamp = '2016-08-03'
    metric = 'auc'
    number = 2
    query = ("      SELECT run_time FROM results.evaluations JOIN results.models \
                    ON evaluations.model_id=models.model_id \
                    WHERE run_time >= '{}' \
                    AND value is not null \
                    AND metric = '{}' \
                    ORDER BY value DESC LIMIT {} ; ").format(timestamp, metric, number)
    df_models = pd.read_sql(query, con=engine)
    print(df_models)
    #output = df_models['run_time'].apply(lambda x: str(x).replace(' ', 'T')).values
    #print(output)
'''
