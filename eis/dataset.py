import numpy as np
import pandas as pd
import yaml
import logging
import sys
import psycopg2
import datetime
import json
import pdb

from . import setup_environment
from .features import class_map

# inherit format from root level logger (specified in eis/run.py)
log = logging.getLogger(__name__)

try:
    engine = setup_environment.get_database()
    db_conn = engine.raw_connection()
    log.debug('Connected to the database')
except:
    log.warning('Could not connect to the database')
    raise


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

def store_model_info( timestamp, batch_comment, batch_timestamp, config, pickle_obj="", pickle_file="" ):
    """ Write model configuration into the results.model table

    :param str timestamp: the timestamp at which this model was run.
    :param str batch_comment: the user-defined comment string.
    :param str batch_timestamp: the timestamp that this batch of models was run.
    :param dict config: the configuration dictionary that contains all model parameters.
    :param str pickle_obj: the serialized pickle object string for this model run.
    :param str pickle_file: the path and name of the pickle file.
    """

    # set some parameters model comment.
    model_comment = "" # TODO: feature not implemented, should read from config.
    # insert into the models table.
    query = (    " INSERT INTO results.models( run_time, batch_run_time, model_type, model_parameters, model_comment, batch_comment, config, pickle_file_path_name ) "
                 " VALUES(  %s, %s, %s, %s, %s, %s, %s, %s )" )
    db_conn.cursor().execute(query, (   timestamp,
                                        batch_timestamp,
                                        config["model"],
                                        json.dumps(config["parameters"]),
                                        model_comment,
                                        batch_comment,
                                        json.dumps(config),
                                        pickle_file ) )
    db_conn.commit()

    # if a pickle object was passed in, insert into the data table.

    if pickle_obj:

        # get the model primary key corresponding to this entry, based on timestamp.
        query = ( " SELECT model_id FROM results.models WHERE models.run_time = '{}'::timestamp ".format( timestamp ) )
        cur = db_conn.cursor()
        cur.execute(query)
        this_model_id = cur.fetchone()
        this_model_id = this_model_id[0]

        # insert into the data table.
        query = ( "INSERT INTO results.data( model_id, pickle_blob ) VALUES( %s, %s )" )
        db_conn.cursor().execute(query, ( this_model_id, psycopg2.Binary(pickle_obj) ) )
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

def store_prediction_info( timestamp, unit_id_train, unit_id_test, unit_predictions, unit_labels, store_as_csv=False ):
    """ Write the model predictions (officer or dispatch risk scores) to the results schema.

    :param str timestamp: the timestamp at which this model was run.
    :param list unit_id_train: list of unit id's used in the training set.
    :param list unit_id_test: list of unit id's used in the test set.
    :param list unit_predictions: list of risk scores.
    :param list unit_labels: list of true labels.
    :param bool store_as_csv: if True, skip insert into predictions table, and store as a csv file instead.
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
                                            "unit_id": unit_id_test,
                                            "unit_score": unit_predictions,
                                            "label_value": unit_labels } )

    if store_as_csv:
        # hack-y: much faster to write to csv, then read csv with psql than to write straight to database from python
        csv_filepath = "{}/{}_{}.csv".format('results', 'dispatch_results', timestamp)
        dataframe_for_insert.to_csv(csv_filepath, index=False)
        # TODO: write code to actually load the csv into postgres with psql
    else:
        dataframe_for_insert.to_sql( "predictions", engine, if_exists="append", schema="results", index=False )

    return None

#def store_evaluation_metrics( timestamp, evaluation_metrics ):
def store_evaluation_metrics( timestamp, evaluation, metric, parameter=None, comment=None):
    """ Write the model evaluation metrics into the results schema

    :param str timestamp: the timestamp at which this model was run.
    :param dict evaluation_metrics: dictionary whose keys correspond to metric names in the features.evaluations columns.
    """

    # get the model primary key corresponding to this timestamp.
    query = ( " SELECT model_id FROM results.models WHERE models.run_time = '{}'::timestamp ".format( timestamp ) )
    cur = db_conn.cursor()
    cur.execute(query)
    this_model_id = cur.fetchone()
    this_model_id = this_model_id[0]

    #No parameter and no comment
    if parameter is None and comment is None:
        comment = 'Null'
        parameter = 'Null'
        query = (   "   INSERT INTO results.evaluations( model_id, metric, parameter, value, comment)"
                    "   VALUES( '{}', '{}', {}, '{}', {}) ".format( this_model_id,
                                                                    metric,
                                                                    parameter,
                                                                    evaluation,
                                                                    comment ) )

    #No parameter and a comment
    elif parameter is None and comment is not None:
        parameter = 'Null'
        query = (   "   INSERT INTO results.evaluations( model_id, metric, parameter, value, comment)"
                    "   VALUES( '{}', '{}', {}, '{}', '{}') ".format( this_model_id,
                                                                    metric,
                                                                    parameter,
                                                                    evaluation,
                                                                    comment ) )

    #No comment and a parameter
    elif parameter is not None and comment is None:
        comment = 'Null'
        query = (   "   INSERT INTO results.evaluations( model_id, metric, parameter, value, comment)"
                    "   VALUES( '{}', '{}', '{}', '{}', {}) ".format( this_model_id,
                                                                    metric,
                                                                    parameter,
                                                                    evaluation,
                                                                    comment ) )

    #A comment and a parameter
    elif parameter is not None and comment is not None:
        query = (   "   INSERT INTO results.evaluations( model_id, metric, parameter, value, comment)"
                    "   VALUES( '{}', '{}', '{}', '{}', '{}') ".format( this_model_id,
                                                                    metric,
                                                                    parameter,
                                                                    evaluation,
                                                                    comment ) )
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


    def officer_labeller(self, officer_labels, ids_to_label=None):
        """
        Load the IDs for a set of officers who are 'active' during the supplied time window
        and generate 0 / 1 labels for them. The definition of 'active' is determined by the
        options passed in the 'labelling' dictionary.

        Inputs:
        officer_labels: dict of bools representing which event types are considered adverse for
                        the purposes of prediction, and also what mask to apply to officers
                        with respect to whether or not they are active.
        ids_to_label: (Optional) a list of officer_ids to return labels for. Note that if a
                      given officer_id is not included in [start_date, end_date] it will
                      not be in the returned dataframe.

        Returns:
        outcomes: pandas dataframe with two columns: officer_id and adverse_by_ourdef
        """

        log.info("Loading labels...")

        # select all officer ids which we will use for labelling
        query_all_officers = (          "SELECT DISTINCT officer_id "
                                        "FROM staging.events_hub AS events_hub "
                                        "LEFT JOIN staging.internal_affairs_investigations AS ia_table "
                                        "   ON events_hub.event_id = ia_table.event_id "
                                        "WHERE events_hub.event_datetime > '{}'::date "
                                        "AND events_hub.event_datetime <= '{}'::date "
                                        .format(
                                            self.start_date,
                                            self.end_date))

        if officer_labels['include_all_active'] == True:

            # add the officer_ids of officers who show up in the arrests, traffic, and pedestrian stops tables
            # see lookup_event_types for the explanation of (1, 2, 3)
            query_all_officers += (         "UNION "
                                        "SELECT DISTINCT officer_id FROM staging.events_hub "
                                        "WHERE event_type_code in (1, 2, 3, 6) "
                                        "AND event_datetime > '{}' "
                                        "AND event_datetime <= '{}' "
                                        .format(
                                          self.start_date,
                                          self.end_date))

        # create the query to find all officer ID's associated with an adverse incident.
        query_base = (  "SELECT incidents.officer_id "
                        "FROM staging.events_hub "
                        "JOIN staging.incidents "
                        "ON staging.events_hub.event_id = staging.incidents.event_id " )

        # create the query to mask in time.
        query_time =  (  "WHERE events_hub.event_datetime > '{}'::date "
                         "AND events_hub.event_datetime <= '{}'::date "
                         .format(    self.end_date,
                                    self.end_label_date ) )

        # set the individual queries for each type of label.
        query_sustained = "final_ruling_code in ( 1, 4, 5 ) "
        query_sustained_and_unknown_outcome = "final_ruling_code in (0, 1, 4, 5 ) "
        query_all       = "number_of_allegations > 0 "
        query_major     = "grouped_incident_type_code in ( 0, 2, 3, 4, 8, 9, 10, 11, 17, 20 ) "
        query_minor     = "grouped_incident_type_code in ( 1, 6, 16, 18, 12, 7, 14 ) "
        query_force     = "grouped_incident_type_code = 20 "
        query_unknown   = "grouped_incident_type_code = 19 "

        # construct the query to get officer ID's with adverse incidents as defined by the user.
        queries_for_adverse = []
        if officer_labels["ForceAllegations"]:
            queries_for_adverse.append( query_force  )

        if officer_labels["SustainedForceAllegations"]:
            queries_for_adverse.append( query_force + " AND " +  query_sustained )

        if officer_labels["SustainedandUnknownForceAllegations"]:
            queries_for_adverse.append( query_force + " AND " +  query_sustained_and_unknown_outcome )

        if officer_labels["AllAllegations"]:
            queries_for_adverse.append( query_all )

        if officer_labels["SustainedAllegations"]:
            queries_for_adverse.append( query_sustained )

        if officer_labels["SustainedandUnknownOutcomeAllegations"]:
            queries_for_adverse.append( query_sustained_and_unknown_outcome )

        if officer_labels["MajorAllegations"]:
            queries_for_adverse.append( query_major )

        if officer_labels["SustainedMajorAllegations"]:
            queries_for_adverse.append( query_major + " AND " + query_sustained )

        if officer_labels["SustainedUnknownMajorAllegations"]:
            queries_for_adverse.append( query_major + " AND " + query_sustained_and_unknown_outcome )

        if officer_labels["MinorAllegations"]:
            queries_for_adverse.append( query_minor )

        if officer_labels["SustainedMinorAllegations"]:
            queries_for_adverse.append( query_minor + " AND " + query_sustained )

        if officer_labels["SustainedUnkownMinorAllegations"]:
            queries_for_adverse.append( query_minor + " AND " + query_sustained_and_unknown_outcome )

        if officer_labels["UnknownAllegations"]:
            queries_for_adverse.append( query_unknown )

        if officer_labels["SustainedUnknownAllegations"]:
            queries_for_adverse.append( query_unknown + " AND " + query_sustained )

        if officer_labels["SustainedUnknownUnknownAllegations"]:
            queries_for_adverse.append( query_unknown + " AND " + query_sustained_and_unknown_outcome )

        # join together the adverse queries into a single mask.
        if len(queries_for_adverse) > 0:
            query_adverse = " ( " + " ) \n OR ( ".join(queries_for_adverse) + " ) "
        else:
            query_adverse = ""

        # setup the full query for getting the labels.
        if query_adverse:
            query_labels = query_base + query_time + " AND ( " + query_adverse + " ) "
        else:
            query_labels = query_base + query_time

        # pull in all the officer_ids to use for labelling
        all_officers = pd.read_sql(query_all_officers, con=db_conn).drop_duplicates()

        # pull in the officer_ids of officers who had adverse incidents
        adverse_officers = pd.read_sql(query_labels, con=db_conn).drop_duplicates()
        adverse_officers["adverse_by_ourdef"] = 1

        # merge the labelled and adverse officer_ids and fill in the non-adverse rows with 0s
        outcomes = adverse_officers.merge(all_officers, how='outer', on='officer_id')
        outcomes = outcomes.fillna(0)

        log.debug('... number of officers in set : {}'.format(len(all_officers)))
        log.debug('... number of officers with adverse incidents : {}'.format(len(adverse_officers)))

        # if given a list of officer ids to label, exclude officer_ids not in that list
        if ids_to_label is not None:
            outcomes = outcomes.loc[outcomes.officer_id.isin(ids_to_label)]

        return outcomes

    def load_all_features(self, features_to_load, ids_to_use=None, feature_type='officer'):
        """Get the feature values from the database

        Args:
            features_to_load(list): names of all features to be loaded, names must be in classmap
            ids_to_use(list): the subset of ids to return feature values for
            feature_type(str): the type of feature being loaded, one of ['officer', 'dispatch']

        Returns:
            returns(pd.DataFrame): dataframe of the feature values indexed by officer_id or dispatch_id
            """


        feature_name_list = ', '.join(features_to_load)

        # select the appropriate id column and feature table name for this feature type
        if feature_type == 'officer':
            id_column = 'officer_id'
        if feature_type == 'dispatch':
            id_column = 'dispatch_id'

        # Create the query for this feature list
        query = (   "SELECT {}, {} "
                    "FROM features.{} "
                    "WHERE as_of_date > '{}' AND as_of_date <= '{}'"
                    .format(
                        id_column,
                        feature_name_list,
                        self.table_name,
                        self.start_date,
                        self.end_date))

        # Execute the query.
        results = self.__read_feature_table(query, id_column)

        # filter dispatch-level features for officer-initiated dispatches.
        if feature_type == "dispatch":
            results = self.__filter_dispatch_features( results )

        # filter out the rows which aren't in ids_to_use
        if ids_to_use is not None:
            results = results.ix[ids_to_use]

        return results

    def __read_feature_table(self, query, id_column, drop_duplicates=True, drop_OI=True, has_geolocation=True):
        """Return a dataframe with data from the features table, indexed by the relevant id (officer or dispatch)"""

        log.debug("Loading features for events from {} to {}".format(
                        self.start_date, self.end_date))

        # Load this feature from the feature table.
        results = pd.read_sql(query, con=db_conn)

        if drop_duplicates:
            results = results.drop_duplicates(subset=[id_column])

        # index by the relevant id
        results = results.set_index(id_column)

        # -1 in feature count is b/c 'label' is also a column, but not a feature
        log.debug("... {} rows, {} features".format(len(results),
                                                    len(results.columns)))
        return results


    def __filter_dispatch_features(self, results, drop_OI=True, has_geolocation=True):
        """ Filter dispatch features for officer-initiated dispatches """

        # Remove dispatches that are officer initiated
        if drop_OI:
            results = results.loc[results.dispatchcategory != "OI"]

        # Remove dispatches that do not have geolocation (percentage black in census tract is 
        # a proxy as it will be assigned to all with a geolocation)
        if has_geolocation:
            results = results[~results.percentageblackinct.isnull()]

        return results


def get_dataset(start_date, end_date, prediction_window, officer_past_activity_window, features_list,
                label_list, features_table, labels_table):

    features_list = [ feature.lower() for feature in features_list]
    features_list_string = ", ".join(['"{}"'.format(feature) for feature in features_list])
    label_list_string = ", ".join(["'{}'".format(label) for label in label_list])

    query_labels = (""" WITH features_labels as ( """
                    """     SELECT officer_id, {0}, """
                    """             as_of_date, """ 
                    """             coalesce(outcome,0) as outcome """
                    """     FROM features.{2} f """
                    """     LEFT JOIN LATERAL ( """
                    """           SELECT 1 as outcome """
                    """           FROM features.{3} l """
                    """           WHERE f.officer_id = l.officer_id """
                    """                AND l.outcome_timestamp - INTERVAL '{4}months' <= f.as_of_date """
                    """                AND l.outcome_timestamp > f.as_of_date """
                    """                AND outcome in ({1}) LIMIT 1"""
                    """                 ) AS l ON TRUE """
                    """     WHERE f.as_of_date >= '{5}'::date AND f.as_of_date < '{6}' ) """
                      .format(features_list_string,
                              label_list_string,
                              features_table,
                              labels_table,
                              prediction_window,
                              start_date,
                              end_date))

    query_active =  (""" SELECT officer_id, {0}, outcome """
                    """ FROM features_labels as f, """
                    """        LATERAL """
                    """          (SELECT 1 """
                    """           FROM staging.events_hub e """
                    """           WHERE f.officer_id = e.officer_id """
                    """           AND e.event_datetime + INTERVAL '{1}months' > f.as_of_date """
                    """           AND e.event_datetime <= f.as_of_date """
                    """            LIMIT 1 ) sub; """
                    .format(features_list_string,
                            officer_past_activity_window))
    query = (query_labels + query_active)
    all_data = pd.read_sql(query, con=db_conn)

    # fill missing values with zero
    all_data = all_data.fillna(0)
    # remove rows with only zero values
    all_data = all_data.loc[~(all_data==0).all(axis=1)]

    all_data = all_data.set_index('officer_id')
    return all_data[features_list], all_data.outcome

def grab_officer_data(features, start_date, end_date, end_label_date, labelling, table_name ):
    """
    Function that defines the dataset.

    Inputs:
    -------
    features: list containing which features to use
    start_date: start date for selecting officers
    end_date: end date for selecting officers
    end_label_date: build features with respect to this date
    labelling: dict containing options to label officers
    by IA should be labelled, if False then only those investigated will
    be labelled
    """

    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    log.debug("Calling FeatureLoader with: {},{},{},{}".format(start_date, end_date, end_label_date, table_name))
    data = FeatureLoader(start_date, end_date, end_label_date, table_name)

    # get the officer labels and make them the key in the dataframe.
    officers = data.officer_labeller(labelling)
    officers.officer_id = officers.officer_id.astype(int)

    # select all the features which are set to True in the config file
    features_data = data.load_all_features( features, feature_type="officer" )

    # join the data to the officer labels.
    dataset = officers
    dataset = dataset.join( features_data, how='left', on="officer_id" )
    dataset.set_index(["officer_id"], inplace=True )
    dataset = dataset.fillna(0)

    labels = dataset["adverse_by_ourdef"].values
    feats = dataset.drop(["adverse_by_ourdef"], axis=1)
    ids = dataset.index.values

    # make sure we return a non-zero number of labelled officers
    assert sum(labels) > 0, 'Labelled officer selection returned no officers'

    log.debug("Dataset has {} rows and {} features".format(
       len(labels), len(feats.columns)))

    return feats, labels, ids, features


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
