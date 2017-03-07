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
                                            "entity_id": unit_id_test,
                                            "score": unit_predictions,
                                            "as_of_date": my_exp_config['test_end_date'],
                                            "label_value": unit_labels } )
    
    # Add rank columns
    dataframe_for_insert['rank_abs'] = dataframe_for_insert['score'].rank(method='dense', ascending=False)
    dataframe_for_insert['rank_pct'] = dataframe_for_insert['score'].rank(method='dense', ascending=False, pct=True)

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



class FeatureLoader():

    def __init__(self, features, features_table, labels_config, labels, labels_table, prediction_window, officer_past_activity_window, db_engine ):
        '''
        Args:
            features (list): list of features to use for the matrix
            feature_table (str : name of the features table in the db
            labels_config (dict): config file of the conditions for each label
            labels (dict): labels dictionary to use from the config file
            prediction_window (str) : prediction window to use for the label generation
            officer_past_activity_window (str): window for conditioning which officers to use given an as_of_date
        '''

        self.features = features
        self.features_table = features_table
        self.labels_config = labels_config
        self.labels = labels
        self.labels_table = labels_table
        self.prediction_window = prediction_window
        self.officer_past_activity_window = officer_past_activity_window
        self.db_con = db_engine.connect()
        self.flatten_label_keys = [item for sublist in self.labels for item in sublist]

    def _tree_conditions(self, nested_dict, parent=[], conditions=[]):
        '''
        Function that returns a list of conditions from the labels config file 
        looping recursively through each tree
        Args:
            nested_dict (dict): dictionary for each of the keys in the labels_config file
            parent (list): use in the recursive function to append the parent to each tree
            conditions (list): use in the recursive mode to append all the conditions
        '''
        if isinstance(nested_dict, dict):
            column = nested_dict['COLUMN']
            for value in nested_dict['VALUES']:
                parent_temp = parent.copy()
                if isinstance(value, dict):
                    for key in value.keys():
                        parent_temp.append('{col}:{val}'.format(col=column, val=key))
                        self._tree_conditions(value[key], parent_temp, conditions)
                else:
                    parent_temp.append('{col}:{val}'.format(col=column, val=value))
                    conditions.append('{{{parent_temp}}}'.format(parent_temp=",".join(parent_temp)))
        return conditions

    def _get_event_type_columns(self, nested_dict, list_events=[]):
        if isinstance(nested_dict, dict):
            list_events.append(nested_dict['COLUMN'])
            for val in nested_dict['VALUES']:
                if isinstance(val, dict):
                    for key in val.keys():
                        self._get_event_type_columns(val[key], list_events)
        return list_events

    def get_query_labels(self, as_of_dates_to_use):
        '''
        '''

        # SUBQUERIES of arrays of conditions
        sub_query = []
        event_type_columns = set()
        for key in self.flatten_label_keys:
            condition = key.lower()
            list_conditions = self._tree_conditions(self.labels_config[key], parent=[], conditions=[])
            sub_query.append(" {condition}_table as "
                            "    ( SELECT  "
                            "          unnest(ARRAY{list_conditions}) as {condition}_condition )"
                            .format(condition=condition,
                                    list_conditions=list_conditions))
            # event type
            event_type_columns.update(self._get_event_type_columns(self.labels_config[key], []))

        # JOIN subqueries
        sub_queries = ", ".join(sub_query)
        sub_queries = ("WITH {sub_queries}, "
                       " all_conditions as "
                       "    (SELECT * "
                       "     FROM {cross_joins})"
                       .format(sub_queries=sub_queries,
                               cross_joins=" CROSS JOIN ".join([key.lower() + '_table' for key in self.flatten_label_keys])))

        # CREATE AND AND OR CONDITIONS
        and_conditions = []
        for and_labels in self.labels:
            or_conditions = []
            for label in and_labels:
                or_conditions.append("event_type_array::text[] @> {key}_condition::text[]".format(key=label.lower()))
            and_conditions.append(" OR ".join(or_conditions))
        conditions = " AND ".join('({and_condition})'.format(and_condition=and_condition) for and_condition in and_conditions) 

        # QUERY OF AS OF DATES
        query_as_of_dates = (" as_of_dates as ( "
                            "select unnest(ARRAY{as_of_dates}::timestamp[]) as as_of_date) "
                            .format(as_of_dates=as_of_dates_to_use))

        # DATE FILTER
        query_filter = ("group_events as ( "
                        "SELECT officer_id,  "
                        "       event_id, "
                        "       array_agg(event_type::text ||':'|| value::text ORDER BY 1) as event_type_array, " 
                        "       min(event_datetime) as min_date, "
                        "       max(event_datetime) filter (where event_type in "
                        "                          (SELECT unnest(ARRAY{event_types}))) as max_date "
                        "FROM features.{labels_table}  "
                        "GROUP BY officer_id, event_id  "
                        "), date_filter as ( "
                        " SELECT  officer_id, "
                        "        as_of_date, "
                        "        event_type_array "
                        " FROM group_events "
                        " JOIN  as_of_dates ON "
                        " min_date > as_of_date and max_date <= as_of_date + INTERVAL '{prediction_window}') "
                        .format(event_types=list(event_type_columns),
                                labels_table=self.labels_table,
                                prediction_window=self.prediction_window))

        query_select_labels = (" labels as ( "
                               "  SELECT officer_id, "
                               "        as_of_date, "
                               "        1 as outcome "
                               " FROM date_filter "
                               " JOIN all_conditions ON "
                               "   {conditions} "
                               " GROUP by as_of_date, officer_id)"
                               .format(conditions=conditions))
       
        # CONCAT all parts of query
        query_labels = ("{sub_queries}, "
                        "{as_of_dates}, "
                        "{query_filter}, "
                        "{query_select}".format(sub_queries=sub_queries,
                                                as_of_dates=query_as_of_dates,
                                                query_filter=query_filter,
                                                query_select=query_select_labels))
        return query_labels

    def get_dataset(self, as_of_dates_to_use):
        '''
        This function returns dataset and labels to use for training / testing
        It is splitted in two queries:
            - features_subquery: which joins the features table with labels table
            - query_active: using the first table created in query_labels, and returns it only 
                            for officers that are have any activity given the officer_past_activity_window
        
        '''

        # convert features to string for querying while replacing NULL values with ceros in sql
        features_coalesce = ", ".join(['coalesce("{0}",0) as {0}'.format(feature) for feature in self.features])
        features_list_string = ", ".join(['{}'.format(feature) for feature in self.features])

        # JOIN FEATURES AND LABELS
        query_features_labels = ( " {labels_subquery}, "
                              " features_labels AS ( "
                              "    SELECT officer_id, "
                              "           as_of_date, "
                              "           {features_coalesce}, "
                              "           coalesce(outcome,0) as outcome "
                              "    FROM features.{features_table} "
                              "    LEFT JOIN labels "
                              "    USING (as_of_date, officer_id) "
                              "    WHERE {features_table}.as_of_date in ( SELECT as_of_date from as_of_dates)) "
                              .format(labels_subquery=self.get_query_labels(as_of_dates_to_use),
                                      features_coalesce=features_coalesce,
                                      features_table=self.features_table))
                       
        # We only want to train and test on officers that have been active (any logged activity in events_hub)
        # NOTE: it uses the feature_labels created in query_labels 
        query_active =  (""" SELECT officer_id, {features}, outcome """
                        """ FROM features_labels as f, """
                        """        LATERAL """
                        """          (SELECT 1 """
                        """           FROM staging.events_hub e """
                        """           WHERE f.officer_id = e.officer_id """
                        """           AND e.event_datetime + INTERVAL '{window}' > f.as_of_date """
                        """           AND e.event_datetime <= f.as_of_date """
                        """            LIMIT 1 ) sub; """
                        .format(features=features_list_string,
                                window=self.officer_past_activity_window))
         
        # join both queries together and load data
        query = (query_features_labels + query_active)
        
        all_data = pd.read_sql(query, con=db_conn)
        
        ## TODO: remove all zero value columns
        #all_data = all_data.loc[~(all_data[features_list]==0).all(axis=1)]
    
        all_data = all_data.set_index('officer_id')
        log.debug('length of data_set: {}'.format(len(all_data)))
        log.debug('number of officers with adverse incident: {}'.format( all_data['outcome'].sum() ))
