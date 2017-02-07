import numpy as np
import pandas as pd
import yaml
import logging
import sys
import psycopg2
import datetime
import json
import pdb
import uuid
import metta

from . import setup_environment
from .features import class_map

# inherit format from root level logger (specified in eis/run.py)
log = logging.getLogger(__name__)

#try:
#    engine = setup_environment.get_database()
#    db_conn = engine.raw_connection()
#    log.debug('Connected to the database')
#except:
#    log.warning('Could not connect to the database')
#    raise


def change_schema(schema):
    db_conn.cursor().execute("SET SCHEMA '{}'".format(schema))
    log.debug('Changed the schema to {}'.format(schema))
    return None

def enter_into_db(timestamp, config, auc):
    query = ("INSERT INTO models.full (id_timestamp, config, auc) "
             "VALUES ('{}', '{}', {}) ".format(timestamp, json.dumps(config), auc))
    db_conn.cursor().execute(query)
    db_conn.commit()
    return None

def generate_matrix_id(config):
    blocks = '-'.join(config["officer_features"])
    time_aggregations = '-'.join(config["timegated_feature_lookback_duration"])
    
    return blocks + ' ' + time_aggregations

def store_matrices(to_save, config):
    date_fmt = "%Y-%m-%d"
    label_name = "_".join(sorted(to_save["config"]["officer_labels"]))
    train_df = pd.DataFrame(to_save["train_x"], columns=to_save["features"], index=to_save["officer_id_train"])
    train_df[label_name] = to_save["train_y"]
    train_config = {'start_time': datetime.datetime.strptime(to_save["config"]["train_start_date"], date_fmt),
                    'end_time': datetime.datetime.strptime(to_save["config"]["train_end_date"], date_fmt),
                    'prediction_window': to_save["config"]["prediction_window"],
                    'label_name': label_name,
                    'feature_names': sorted(to_save["features"].tolist()),
                    'unit_id': to_save["officer_id_train"].tolist(),
                    'matrix_id': generate_matrix_id(config) }

    test_df = pd.DataFrame(to_save["test_x"], columns=to_save["features"], index=to_save["officer_id_test"])
    test_df[label_name] = to_save["test_y"]
    test_config = {'start_time': datetime.datetime.strptime(to_save["config"]["test_start_date"], date_fmt),
                   'end_time': datetime.datetime.strptime(to_save["config"]["test_end_date"], date_fmt),
                   'prediction_window': to_save["config"]["prediction_window"],
                   'label_name': label_name,
                   'feature_names': sorted(to_save["features"].tolist()),
                   'unit_id': to_save["officer_id_test"].tolist(),
                   'matrix_id': generate_matrix_id(config)}

    metta.archive_train_test(train_config, train_df,
                             test_config, test_df,
                             directory = config["directory"],  format = 'hdf5')
    
    return None

def store_model_info( timestamp, batch_comment, batch_timestamp, config, paths={}):
    """ Write model configuration into the results.model table

    :param str timestamp: the timestamp at which this model was run.
    :param str batch_comment: the user-defined comment string.
    :param str batch_timestamp: the timestamp that this batch of models was run.
    :param dict config: the configuration dictionary that contains all model parameters.
    :param str filename: the path and name of the pickle file.
    """

    # set some parameters model comment
    model_comment = config['model_comment'] # TODO: feature not implemented, should read from config.

    # insert into the models table.
    query = (    " INSERT INTO results.models( run_time, batch_run_time, model_type, model_parameters, model_comment, batch_comment, config, test) "
                 " VALUES(  %s, %s, %s, %s, %s, %s, %s, %s )" )

    db_conn.cursor().execute(query, (   timestamp,
                                        batch_timestamp,
                                        config["model"],
                                        json.dumps(config["parameters"]),
                                        model_comment,
                                        batch_comment,
                                        json.dumps(config),
                                        config["test_flag"]
                                        ) )
    db_conn.commit()

    ## add model group_id
    add_model_group_id( timestamp )

    return None

def add_model_group_id(timestamp):
    """ 
    Set model group id in results.models for the model given the same model type, model parameters, prediction window and list of features
    Using the store procedure: get_model_group_id
    
    :param str timestamp: the timestamp at which the model was run 
    """

    query = (" UPDATE results.models " 
             "    SET model_group_id = get_model_group_id(model_type, model_parameters, (config -> 'prediction_window') :: TEXT, "
             "                               ARRAY(SELECT jsonb_array_elements_text(config -> 'officer_features') "
             "                                ORDER BY 1) :: TEXT []) "
             "  WHERE run_time = '{}'::timestamp ".format(timestamp)) 
    db_conn.cursor().execute(query)
    db_conn.commit()

    return None

def store_feature_importances( timestamp, to_save):
    """  Write the feature importances of the model into the results schema

    :param str timestamp: the timestamp at which this model was run.
    :param dict to_save: 
    """

    # get the model primary key corresponding to this timestamp.
    query = ( " SELECT model_id FROM results.models WHERE models.run_time = '{}'::timestamp ".format( timestamp ) )
    cur = db_conn.cursor()
    cur.execute(query)
    this_model_id = cur.fetchone()
    this_model_id = this_model_id[0]

    # Create pandas db of features importance
    dataframe_for_insert = pd.DataFrame( {  "model_id": this_model_id,
                                            "feature": to_save["feature_importances_names"],
                                            "feature_importance": to_save["feature_importances"] })
    # Insert
    dataframe_for_insert.to_sql( "feature_importances", engine, if_exists="append", schema="results", index=False )

    return None

def obtain_top5_risk(row):
    ind = sorted(range(len(row)), key=lambda i: abs(row[i]), reverse=True)[:min(len(row),5) ]
    important_names = [row.index[i] if row[i] > 0 else '-{}'.format(row.index[i]) if row[i] < 0 else None for i in ind ]
    return [ important_names[i] if i < len(important_names) else None for i in range(5) ]
        

def store_individual_feature_importances(timestamp, to_save):

    query = ( " SELECT model_id FROM results.models WHERE models.run_time = '{}'::timestamp ".format( timestamp ) )
    cur = db_conn.cursor()
    cur.execute(query)
    this_model_id = cur.fetchone()
    this_model_id = this_model_id[0]

    # get json of individual feature importances into df
    df_individual_importances = pd.DataFrame(to_save["individual_importances"])
    df_individual_importances.columns = to_save["feature_importances_names"]
    # Obtain the top 5 risks
    df_individual_importances["risks"] = df_individual_importances.apply(lambda x: obtain_top5_risk(x), axis=1)
    df_individual_importances['officer_id_test'] = to_save['officer_id_test']
    df_risks = pd.DataFrame(df_individual_importances["risks"].tolist(), )
    df_risks["unit_id"] = df_individual_importances["officer_id_test"]
    df_risks.columns = ["risk_1", "risk_2","risk_3","risk_4","risk_5","unit_id"]
    df_risks["model_id"] = this_model_id

    df_risks.to_sql( "individual_importances", engine, if_exists="append", schema="results", index=False )

def store_prediction_info( timestamp, unit_id_train, unit_id_test, unit_predictions, unit_labels, my_exp_config ):
    """ Write the model predictions (officer or dispatch risk scores) to the results schema.

    :param str timestamp: the timestamp at which this model was run.
    :param list unit_id_train: list of unit id's used in the training set.
    :param list unit_id_test: list of unit id's used in the test set.
    :param list unit_predictions: list of risk scores.
    :param list unit_labels: list of true labels.
    :param dict my_exp_config: configuration of the experiment
    """

    # get the model primary key corresponding to this timestamp.
    query = ( " SELECT model_id FROM results.models WHERE models.run_time = '{}'::timestamp ".format( timestamp ) )
    cur = db_conn.cursor()
    cur.execute(query)
    this_model_id = cur.fetchone()
    this_model_id = this_model_id[0]

    # round unit_id's to integer type.
    unit_id_train = [int(unit_id) for unit_id in unit_id_train]
    unit_id_test  = [int(unit_id) for unit_id in unit_id_test]
    unit_labels   = [int(unit_id) for unit_id in unit_labels]

    # append data into predictions table. there is probably a faster way to do this than put it into a
    # dataframe and then use .to_sql but this works for now.
    dataframe_for_insert = pd.DataFrame( {  "model_id": this_model_id,
                                            "as_of_date": my_exp_config['test_end_date'],
                                            "unit_id": unit_id_test,
                                            "unit_score": unit_predictions,
                                            "label_value": unit_labels } )
    
    # Add rank columns
    dataframe_for_insert['rank_abs'] = dataframe_for_insert['unit_score'].rank(method='dense', ascending=False)
    dataframe_for_insert['rank_pct'] = dataframe_for_insert['unit_score'].rank(method='dense', ascending=False, pct=True)

    dataframe_for_insert.to_sql( "predictions", engine, if_exists="append", schema="results", index=False )

    return None

def store_evaluation_metrics( model_id, evaluation, metric, test_date, db_conn, parameter=None, comment=None):
    """ Write the model evaluation metrics into the results schema

    :param int model_id: the model_id of the model.
    :param dict evaluation_metrics: dictionary whose keys correspond to metric names in the features.evaluations columns.
    :param test_date: date in string 'Y-m-d' for which the test was made
    """

    #No parameter and no comment
    if parameter is None and comment is None:
        comment = 'Null'
        parameter = 'Null'
        query = (   "   INSERT INTO results.evaluations( model_id, metric, parameter, value, comment, as_of_date)"
                    "   VALUES( '{}', '{}', {}, '{}', {}, '{}'::timestamp) ".format( model_id,
                                                                    metric,
                                                                    parameter,
                                                                    evaluation,
                                                                    comment,
                                                                    test_date ) )

    #No parameter and a comment
    elif parameter is None and comment is not None:
        parameter = 'Null'
        query = (   "   INSERT INTO results.evaluations( model_id, metric, parameter, value, comment, as_of_date)"
                    "   VALUES( '{}', '{}', {}, '{}', '{}', '{}'::timestamp) ".format( model_id,
                                                                    metric,
                                                                    parameter,
                                                                    evaluation,
                                                                    comment,
                                                                    test_date ) )

    #No comment and a parameter
    elif parameter is not None and comment is None:
        comment = 'Null'
        query = (   "   INSERT INTO results.evaluations( model_id, metric, parameter, value, comment, as_of_date)"
                    "   VALUES( '{}', '{}', '{}', '{}', {}, '{}'::timestamp) ".format( model_id,
                                                                    metric,
                                                                    parameter,
                                                                    evaluation,
                                                                    comment,
                                                                    test_date ) )

    #A comment and a parameter
    elif parameter is not None and comment is not None:
        query = (   "   INSERT INTO results.evaluations( model_id, metric, parameter, value, comment, as_of_date)"
                    "   VALUES( '{}', '{}', '{}', '{}', '{}', '{}'::timestamp) ".format( model_id,
                                                                    metric,
                                                                    parameter,
                                                                    evaluation,
                                                                    comment,
                                                                    test_date ) )
    else:
        pass

    db_conn.cursor().execute(query)
    db_conn.commit()
    return None


def format_officer_ids(ids):
    formatted = ["{}".format(each_id) for each_id in ids]
    formatted = ", ".join(formatted)
    return formatted


def get_baseline(start_date, end_date):
    """
    Gets EIS baseline - get officers that are flagged
    by the EIS at any point in the labelling window.

    Inputs:
    ids: officer ids
    start_date: beginning of training period
    end_date: end of training period

    Returns: dataframe with ids of those officers flagged by EIS

    Example:
    df_comparison = get_baseline('2010-01-01', '2011-01-01')
    """

    query_flagged_officers = ("SELECT DISTINCT officer_id from {} "
                                "WHERE date_created >= '{}'::date "
                                "AND date_created <='{}'::date".format(
                                    config["eis_table"],
                                    start_date,
                                    end_date))

    # TODO: have this check the actual 2016 EIS table
    #query_flagged_officers = (  "SELECT DISTINCT officer_id "
    #                            "FROM officers_hub" )

    df_eis_baseline = pd.read_sql(query_flagged_officers, con=con)

    return df_eis_baseline.dropna()


def get_interventions(ids, start_date, end_date):
    """
    Gets whether or not an officer has an intervention after
    the EIS flags them.

    Inputs:
    ids: officer ids
    start_date: beginning of testing period
    end_date: end of testing period

    Returns: dataframe with ids and boolean value corresponding to if an
    officer was intervened on or not in the time period.
    """

    intervened_officers = ("select distinct officer_id from {} "
                        "WHERE intervention != 'No Intervention Required' "
                        "AND datecreated >= '{}'::date "
                        "AND datecreated <='{}'::date "
                        "AND officer_id in ({}) ").format(
                            config["eis_table"],
                            start_date, end_date,
                            format_officer_ids(ids))

    no_intervention_officers = ("select distinct officer_id from {} "
                        "WHERE intervention = 'No Intervention Required' "
                        "AND datecreated >= '{}'::date "
                        "AND datecreated <='{}'::date "
                        "AND officer_id in ({}) ").format(
                            config["eis_table"],
                            start_date, end_date,
                            format_officer_ids(ids))

    # read the data from the database
    df_intervention = pd.read_sql(intervened_officers, con=db_conn)
    df_no_intervention = pd.read_sql(no_intervention_officers, con=db_conn)

    df_intervention["intervention"] = 1
    df_action = df_intervention.merge(df_no_intervention, how='right', on='officer_id')
    df_action = df_action.fillna(0)

    return df_action


def get_labels_for_ids(ids, start_date, end_date):
    """Get the labels for the specified officer_ids, for the specified time period

    Args:

    Returns:
        outcomes(pd.DataFrame):
    """

    # required by FeatureLoader but not used in officer_labeller()
    fake_today = datetime.date.today()

    # load labelling and def_adverse from experiment config file
    exp_config = setup_environment.get_experiment_config()
    officer_labels = exp_config['officer_labels']

    return (FeatureLoader(start_date, end_date, fake_today)
            .officer_labeller(officer_labels, ids_to_label=ids))


def imputation_zero(df, ids):
    fulldf = pd.DataFrame(ids, columns=["officer_id"] + [df.columns[0]])
    df["officer_id"] = df.index
    newdf = df.merge(fulldf, how="right", on="officer_id")
    newdf = newdf.fillna(0)
    newdf[df.columns[0]] = newdf[df.columns[0] + "_x"] + newdf[df.columns[0] + "_y"]
    newdf = newdf.drop([df.columns[0] + "_x", df.columns[0] + "_y"], axis=1)
    newdf = newdf.set_index("officer_id")
    return newdf


def imputation_mean(df, featurenames):
    try:
        newdf = df.set_index("officer_id")
    except:
        newdf = df
    for i in range(len(newdf.columns)):
        this_col = newdf.columns[i]
        imp_dummy_name = "imputation_{}".format(this_col)
        newdf[imp_dummy_name] = newdf[this_col].isnull().map(lambda x: 1 if x == True else 0)
        newdf[this_col] = newdf[this_col].fillna(np.mean(newdf[this_col]))
    return newdf, newdf.columns.values


def convert_series(df):
    onecol = df.columns[0]
    numcols = len(df[onecol].iloc[0])
    newcols = [onecol + '_' + str(i) for i in range(numcols)]

    newdf = pd.DataFrame(columns=newcols, index=df.index)
    for i in range(len(df)):
        newdf.iloc[i] = df[onecol].iloc[i]

    return newdf.astype(int), newcols


def convert_categorical(feature_df, feature_columns):
    """
    Convert a dataframe with columns containing categorical variables to
    a dataframe with dummy variables for each category.

    Args:
        feature_df(pd.DataFrame): A dataframe containing the features, some
                                  of which may be categorical
        feature_columns(list): A list of the feature column names

    Returns:
        feature_df_w_dummies(pd.DataFrame): The features dataframe, but with
                        categorical features converted to dummy variables
    """

    log.info('Converting categorical features to dummy variables')

    # note: feature names will be lowercased when returned from the db
    categorical_features = [name.lower() for name in class_map.find_categorical_features(feature_columns)]

    log.info('... {} categorical features'.format(len(categorical_features)))

    # add dummy variables to feature dataframe
    feature_df_w_dummies = pd.get_dummies(feature_df, columns=categorical_features, sparse=True)

    log.info('... {} dummy variables added'.format(len(feature_df_w_dummies.columns)
                                                   - len(feature_df.columns)))

    return feature_df_w_dummies

class FeatureLoader():

    def __init__(self, start_date=None, end_date=None, end_label_date=None, table_name=None):

        self.start_date = start_date
        self.end_date = end_date
        self.end_label_date = end_label_date
        self.table_name = table_name




def get_dataset( prediction_window, officer_past_activity_window, features_list,
                 label_list, features_table, labels_table, as_of_dates, db_conn):
    '''
    This function returns dataset and labels to use for training / testing
    It is splitted in two queries:
        - query_labels: which joins the features table with labels table
        - query_active: using the first table created in query_labels, and returns it only 
                        for officers that are have any activity given the officer_past_activity_window
    
    Inputs:
    -------
    prediction_window: months, used for selecting labels proceding the as_of_date in features_table
    officer_past_activity_window: months, to select officers with activity preceding the as_of_date in features_table
    features_list: list of features to use
    label_list: outcome name to use 
    features_table: name of the features table
    labels_table: name of the labels table 
    as_of_dates: list of as_of_dates to use
    '''
    features_list_string = ", ".join(['{}'.format(feature) for feature in features_list])
    label_list_string = ", ".join(["'{}'".format(label) for label in label_list])
    as_of_dates_string = ", ".join(["'{}'".format(as_of_date) for as_of_date in as_of_dates])
    # convert features to string for querying while replacing NULL values with ceros in sql
    features_coalesce = ", ".join(['coalesce("{0}",0) as {0}'.format(feature) for feature in features_list])
    
    # First part of the query that joins the features table with labels table
    #NOTE: The lateral join with LIMIT 1 constraint is used for speed optimization 
    # as we assign a positive label to the officers as soon as there is one designated event in the prediction windows
    query_labels = (""" WITH features_labels as ( """
                    """     SELECT officer_id, {0}, """
                    """             as_of_date, """ 
                    """             coalesce(outcome,0) as outcome """
                    """     FROM features.{2} f """
                    """     LEFT JOIN LATERAL ( """
                    """           SELECT 1 as outcome """
                    """           FROM features.{3} l """
                    """           WHERE f.officer_id = l.officer_id """
                    """                AND l.outcome_timestamp - INTERVAL '{4}' <= f.as_of_date """
                    """                AND l.outcome_timestamp > f.as_of_date """
                    """                AND outcome in ({1}) LIMIT 1"""
                    """                 ) AS l ON TRUE """
                    """     WHERE  f.as_of_date in ({5}) )"""
                      .format(features_coalesce,
                              label_list_string,
                              features_table,
                              labels_table,
                              prediction_window,
                              as_of_dates_string))

    # We only want to train and test on officers that have been active (any logged activity in events_hub)
    # NOTE: it uses the feature_labels created in query_labels 
    query_active =  (""" SELECT officer_id, {0}, outcome """
                    """ FROM features_labels as f, """
                    """        LATERAL """
                    """          (SELECT 1 """
                    """           FROM staging.events_hub e """
                    """           WHERE f.officer_id = e.officer_id """
                    """           AND e.event_datetime + INTERVAL '{1}' > f.as_of_date """
                    """           AND e.event_datetime <= f.as_of_date """
                    """            LIMIT 1 ) sub; """
                    .format(features_list_string,
                            officer_past_activity_window))
     
    # join both queries together and load data
    query = (query_labels + query_active)
    all_data = pd.read_sql(query, con=db_conn)

    # remove rows with only zero values
    features_list = [ feature.lower() for feature in features_list]

    ## TODO: remove all zero value columns
    #all_data = all_data.loc[~(all_data[features_list]==0).all(axis=1)]

    all_data = all_data.set_index('officer_id')
    log.debug('length of data_set: {}'.format(len(all_data)))
    return all_data #all_data[features_list], all_data.outcome


def grab_dispatch_data(features, start_date, end_date, def_adverse, table_name):
    """Function that returns the dataset to use in an experiment.

    Args:
        features: dict containing which features to use
        start_date(datetime.datetime): start date for selecting dispatch
        end_date(datetime.datetime): end date for selecting dispatch
        def_adverse: dict containing options for adverse incident definitions
        labelling: dict containing options to select which dispatches to use for labelling

    Returns:
        feats(pd.DataFrame): the array of features to train or test on
        labels(pd.DataFrame): n x 1 array with 0 / 1 adverse labels
        ids(pd.DataFrame): n x 1 array with dispatch ids
        featnames(list): the list of feature column names
    """

    feature_loader = FeatureLoader(start_date = start_date,
                                   end_date = end_date,
                                   table_name = table_name)

    # select all the features which are set to True in the config file
    # NOTE: dict.items() is python 3 specific. for python 2 use dict.iteritems()
    features_to_use = [feat for feat, is_used in features.items() if is_used]

    features_df = feature_loader.load_all_features(features_to_use,
                                                   feature_type = 'dispatch')
    # encode categorical features with dummy variables, and fill NAN with 0s
    dataset = convert_categorical(features_df, features_to_use)
    dataset = dataset.fillna(0)

    log.debug('... dataset dataframe is {} GB'.format(dataset.memory_usage().sum() / 1E9))

    # determine which labels to use based on def_adverse
    # note: need to lowercase the column name b/c postgres lowercases everything
    label_columns = [col.lower() for col in class_map.find_label_features(features_to_use)]
    def_adverse_to_label = {'accidents': 'LabelPreventable',
                            'useofforce': 'LabelUnjustified',
                            'complaint': 'LabelSustained'}
    label_cols_to_use = [def_adverse_to_label[key].lower() for key, is_true in def_adverse.items() if is_true]

    # select the active label columns, sum for each row, and set true when sum > 0
    labels = dataset[label_cols_to_use].sum(axis=1).apply(lambda x: x > 0)

    features = dataset.drop(label_columns, axis=1)
    ids = dataset.index
    feature_names = features.columns

    # make sure we return a non-zero number of labelled dispatches
    assert sum(labels.values) > 0, 'No dispatches were labelled adverse'

    log.debug("Dataset has {} rows and {} features".format(
       len(labels), len(feature_names)))

    return features, labels, ids, feature_names
