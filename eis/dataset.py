import numpy as np
import pdb
import pandas as pd
import yaml
import logging
import sys
import datetime
import json
import psycopg2
from IPython.core.debugger import Tracer

from . import setup_environment
from .features import class_map

# inherit format from root level logger (specified in eis/run.py)
log = logging.getLogger(__name__)

try:
    engine, db_config = setup_environment.get_database()
    db_conn = engine.raw_connection()
    log.debug('Connected to the database')
except:
    log.warning('Could not connect to the database')
    raise

try:
    db_conn.cursor().execute("SET SCHEMA '{}'".format(db_config['schema']))
    log.debug('Changed the schema to ', db_config['schema'])
except:
    log.warning('Could not set the database schema')
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

def store_model_info( timestamp, batch_timestamp, config, pickle_data ):
    """ Write model configuration into the results.model table

    :param str timestamp: the timestamp at which this model was run.
    :param str batch_timestamp: the timestamp that this batch of models was run.
    :param dict config: the configuration dictionary that contains all model parameters.
    :param str pkl_obj: the serialized pickle object string for this model run.
    """

    query = ( "INSERT INTO results.models( run_time, batch_run_time, config, pickle_file ) VALUES( %s, %s, %s, %s )" )
    db_conn.cursor().execute(query, ( timestamp, batch_timestamp, json.dumps(config), psycopg2.Binary(pickle_data) ) )
    db_conn.commit()
    return None

def store_prediction_info( timestamp, unit_id_train, unit_id_test, unit_predictions, unit_labels ):
    """ Write the model predictions (officer or dispatch risk scores) to the results schema.

    :param str timestamp: the timestamp at which this model was run.
    :param list unit_id_train: list of unit id's used in the training set.
    :param list unit_id_test: list of unit id's used in the test set.
    :param list unit_predictions: list of risk scores.
    :param list unit_labels: list of true labels.
    """

    # get the model primary key corresponding to this timestamp.
    query = ( " SELECT model_id FROM results.models WHERE models.run_time = '{}'::timestamp ".format( timestamp ) )
    cur = db_conn.cursor()
    cur.execute(query)
    this_model_id = cur.fetchone()
    this_model_id = this_model_id[0]

    # round unit_id's to integer type.
    unit_id_train = list( map( int, unit_id_train ) )
    unit_id_test  = list( map( int, unit_id_test ) )
    unit_labels   = list( map( int, unit_labels ) )

    # append data into predictions table. there is probably a faster way to do this than put it into a 
    # dataframe and then use .to_sql but this works for now.
    dataframe_for_insert = pd.DataFrame( {  "model_id": [this_model_id]*len(unit_id_test), 
                                            "unit_id": unit_id_test,
                                            "unit_score": unit_predictions,
                                            "label_value": unit_labels } )
                                            
    dataframe_for_insert.to_sql( "predictions", engine, if_exists="append", schema="results", index=False ) 
    return None

def store_evaluation_metrics( timestamp, evaluation_metrics ):
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

    # create query and insert into the evaluations table.
    columns = list(evaluation_metrics.keys())
    values  = list(evaluation_metrics.values())
    query = (   " INSERT INTO results.evaluations( model_id, " + ",".join(map(str, columns ) ) + " ) " + 
                " VALUES( " + str(this_model_id) + ", " + ",".join( map( str, values )) + " ) " )
    

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
    labelling = exp_config['labelling']
    def_adverse = exp_config['def_adverse']
    
    return (FeatureLoader(start_date, end_date, fake_today)
            .officer_labeller(labelling, def_adverse, ids_to_label=ids))
           

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

    log.info('Converting categorical features to dummy variablles')

    categorical_features = class_map.find_categorical_features(feature_columns)

    log.info('... {} categorical features'.format(len(categorical_features)))

    # add dummy variables to feature dataframe
    feature_df_w_dummies = pd.get_dummies(feature_df, columns=categorical_features, sparse=True)

    log.info('... {} dummy variables added'.format(len(feature_df_w_dummies.columns) 
                                                   - len(feature_df.columns)))

    return feature_df_w_dummies


class FeatureLoader():

    def __init__(self, start_date=None, end_date=None, fake_today=None, table_name=None):

        self.start_date = start_date
        self.end_date = end_date
        self.fake_today = fake_today
        self.tables = db_config  # Dict of tables
        self.schema = db_config['schema']
        self.table_name = table_name

        change_schema(self.schema)

    def officer_labeller(self, labelling, def_adverse, ids_to_label=None):
        """
        Load the IDs for a set of officers who are 'active' during the supplied time window
        and generate 0 / 1 labels for them. The definition of 'active' is determined by the 
        options passed in the 'labelling' dictionary.

        Inputs:
        labelling: dict of bools representing how officers should be selected for labelling
                        include_all_employed: include all officers who are active according 
                                              to their employment status
                        include_all_active: include all officers who show up in the stops 
                                            or arrests tables
        def_adverse: dict of bools representing which event types are considered adverse for 
                     the purposes of prediction
        ids_to_label: (Optional) a list of officer_ids to return labels for. Note that if a 
                      given officer_id is not included in [start_date, end_date] it will 
                      not be in the returned dataframe.

        Returns:
        outcomes: pandas dataframe with two columns: officer_id and adverse_by_ourdef
        """

        log.info("Loading labels...")

        # select the set of officer ids which we will use for labelling
        query_to_label = (              "SELECT DISTINCT officer_id "
                                        "FROM {} AS events_hub "
                                        "LEFT JOIN {} AS ia_table "
                                        "   ON events_hub.event_id = ia_table.event_id "
                                        "WHERE events_hub.event_datetime >= '{}'::date "
                                        "AND events_hub.event_datetime <= '{}'::date "
                                        .format(
                                            'events_hub',
                                            self.tables['si_table'],
                                            self.start_date,
                                            self.end_date))

        if labelling['include_all_active'] == True:

            # add the officer_ids of officers who show up in the arrests, traffic, and pedestrian stops tables
            # see lookup_event_types for the explanation of (1, 2, 3)
            query_to_label += (         "UNION "
                                        "SELECT DISTINCT officer_id FROM {} "
                                        "WHERE event_type_code in (1, 2, 3) "
                                        "AND event_datetime >= '{}' "
                                        "AND event_datetime <= '{}' "
                                        .format(
                                          'events_hub',
                                          self.start_date,
                                          self.end_date))


        # TODO: add code for labelling['include_all_employed'] option

        # repeat the query above, but only select officers who had incidents
        # jugded to be adverse. 
        query_adverse = (               "SELECT officer_id "
                                        "FROM events_hub "
                                        "LEFT JOIN incidents "
                                        "   ON events_hub.event_id = incidents.event_id "
                                        "WHERE events_hub.event_datetime >= '{}'::date "
                                        "AND events_hub.event_datetime <= '{}'::date "
                                        "AND number_of_sustained_allegations > 0 "
                                        .format(self.start_date,
                                                self.end_date))

#        # add exclusions to the adverse query based on the definition 
#        # of 'adverse' supplied in the experiment file
#        if def_adverse['accidents'] == False:
#            query_adverse = query_adverse + "AND value != 'accident' "
#
#        if def_adverse['useofforce'] == False:
#            query_adverse = query_adverse + "AND value != 'use_of_force' "
#
#        if def_adverse['injury'] == False:
#            query_adverse = query_adverse + "AND value != 'injury' "
#
#        if def_adverse['icd'] == False:
#            query_adverse = query_adverse + "AND value != 'in_custody_death' "
#
#        if def_adverse['nfsi'] == False:
#            query_adverse = query_adverse + "AND value != 'no_force_subject_injury' "
#
#        if def_adverse['dof'] == False:
#            query_adverse = query_adverse + "AND value != 'discharge_of_firearm' "
#
#        if def_adverse['raid'] == False:
#            query_adverse = query_adverse + "AND value != 'raid' "
#
#        if def_adverse['pursuit'] == False:
#            query_adverse = query_adverse + "AND value != 'pursuit' "
#
#        if def_adverse['complaint'] == False:
#            query_adverse = query_adverse + "AND value != 'complaint' "

        # pull in all the officer_ids to use for labelling
        labelled_officers = pd.read_sql(query_to_label, con=db_conn).drop_duplicates()

        # pull in the officer_ids of officers who had adverse incidents
        adverse_officers = pd.read_sql(query_adverse, con=db_conn).drop_duplicates()
        adverse_officers["adverse_by_ourdef"] = 1

        # merge the labelled and adverse officer_ids and fill in the non-adverse rows with 0s
        outcomes = adverse_officers.merge(labelled_officers, how='outer', on='officer_id')
        outcomes = outcomes.fillna(0)

        log.debug('... number of officers to label: {}'.format(len(labelled_officers)))
        log.debug('... number of officers with adverse : {}'.format(len(adverse_officers)))

        # if given a list of officer ids to label, exclude officer_ids not in that list
        if ids_to_label is not None:
            outcomes = outcomes.loc[outcomes.officer_id.isin(ids_to_label)]

        return outcomes


    def dispatch_labeller(self, def_adverse):
        """
        Load the dispatch events which occured between two dates and their outcomes

        Note that this uses the start_date and stop_date attributes of the FeatureLoader
        to limit which dispatches are used.

        Args:
            def_adverse: dict of bools representing which event types are considered adverse for 
                         the purposes of prediction

        Returns:
            labels: pandas dataframe with two columns, dispatch_id and adverse_by_ourdef
        """

        log.debug("Loading dispatch labels...")

        # select all dispatches within the specified time window
        query_all = (       "SELECT DISTINCT dispatch_id "
                            "FROM events_hub "
                            "WHERE events_hub.event_type_code = 5 "
                            "AND events_hub.event_datetime >= '{}'::date "
                            "AND events_hub.event_datetime <= '{}'::date "
                            .format(self.start_date,
                                    self.end_date))

        # select dispatches that led to events deemed adverse
        query_adverse = (   "SELECT DISTINCT dispatch_id "
                            "FROM events_hub "
                            "LEFT JOIN incidents AS incidents "
                            "   ON events_hub.event_id = incidents.event_id "
                            "LEFT JOIN lookup_incident_types AS lookup "
                            "   ON lookup.code = incidents.grouped_incident_type_code "
                            "WHERE event_type_code = 4 "
                            "   AND events_hub.event_datetime >= '{}'::date "
                            "   AND events_hub.event_datetime <= '{}'::date "
                            "   AND (  number_of_unjustified_allegations > 0"
                            "       OR number_of_preventable_allegations > 0"
                            "       OR number_of_sustained_allegations > 0)"
                            .format(self.start_date,
                                    self.end_date))

        # add exclusions to the adverse query based on the definition 
        # of 'adverse' supplied in the experiment file

        # TODO: implement filtering on event type that works with new incidents table
        #if def_adverse['accidents'] == False:
        #    query_adverse = query_adverse + "AND value != 'accident' "

        dispatches = pd.read_sql(query_all, con=db_conn)
        adverse_dispatches = pd.read_sql(query_adverse, con=db_conn)

        log.debug('... number of dispatches to label: {}'.format(len(dispatches)))
        log.debug('... number of dispatches with adverse : {}'.format(len(adverse_dispatches)))

        # fill all the dispatch labels with 0s, then add 1s for the adverse incidents
        dispatches['adverse_by_ourdef'] = 0
        dispatches.ix[adverse_dispatches.index] = 1

        return dispatches

    def loader(self, feature_to_load, ids_to_use, feature_type='officer'):
        """Get the feature values from the database
        
        Args:
            feature_to_load(str): name of feature to be loaded, must be in classmap
            ids_to_use(list): the subset of ids to return feature values for
            
        Returns:
            returns(pd.DataFrame): dataframe of the feature values indexed by id
            feature_name: the name of the feature
            """

        kwargs = {"fake_today": self.fake_today,
                  "table_name": self.table_name,
                  "feat_time_window": 0}
        #feature = class_map.lookup(feature_to_load, **kwargs)

        # select the appropriate id column for this feature type
        if feature_type == 'officer':
            id_column = 'officer_id'
        if feature_type == 'dispatch':
            id_column = 'dispatch_id'

        # Create the query for this feature.
        query = (   "SELECT {}, {} FROM features.{}"
                    .format(
                        id_column,
                        feature_to_load, 
                        self.table_name))
    
        # Execute the query.
        results = self.__read_feature_table(query, id_column)
        results = results.ix[ids_to_use]

        return results, feature_to_load 


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

        # Create the query for this feature.
        query = (   "SELECT {}, {} "
                    "FROM features.{} "
                    .format(
                        id_column,
                        feature_name_list,
                        self.table_name))

        # Execute the query.
        results = self.__read_feature_table(query, id_column)
    
        # filter out the rows which aren't in ids_to_use
        if ids_to_use is not None:
            results = results.ix[ids_to_use]

        log.debug("... {} rows, {} features".format(len(results),
                                                    len(results.columns)))

        return results


    def __read_feature_table(self, query, id_column, drop_duplicates=True):
        """Return a dataframe with data from the features table, indexed by the relevant id (officer or dispatch)"""

        log.debug("Loading features for events from {} to {}".format(
                        self.start_date, self.end_date))

        # Load this feature from the feature table.
        results = pd.read_sql(query, con=db_conn)

        if drop_duplicates:
            results = results.drop_duplicates(subset=[id_column])

        # index by the relevant id
        results = results.set_index(id_column)

        return results


# TODO: make this use load_all_features() instead of loader()
def grab_officer_data(features, start_date, end_date, time_bound, def_adverse, labelling, table_name ):
    """
    Function that defines the dataset.

    Inputs:
    -------
    features: dict containing which features to use
    start_date: start date for selecting officers
    end_date: end date for selecting officers
    time_bound: build features with respect to this date
    def_adverse: dict containing options for adverse incident definitions
    labelling: dict containing options to label officers
    by IA should be labelled, if False then only those investigated will
    be labelled
    """

    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    data = FeatureLoader(start_date, end_date, time_bound, table_name)

    officers = data.officer_labeller(labelling, def_adverse)
    officers.set_index(["officer_id"])

    
    dataset = officers
    featnames = []
    for each_feat in features:
        feature_df, names = data.loader(each_feat,
                                        dataset["officer_id"])
        log.info("Loaded feature {} with {} rows".format(
            each_feat, len(feature_df)))
        featnames = list(featnames) + list(names)
        dataset = dataset.join(feature_df, how='left', on='officer_id')

    dataset.set_index(["officer_id"], inplace=True)
    dataset = dataset.fillna(0)

    labels = dataset["adverse_by_ourdef"].values
    feats = dataset.drop(["adverse_by_ourdef"], axis=1)
    ids = dataset.index.values

    # make sure we return a non-zero number of labelled officers
    assert sum(labels) > 0, 'Labelled officer selection returned no officers'

    log.debug("Dataset has {} rows and {} features".format(
       len(labels), len(feats.columns)))

    return feats, labels, ids, featnames


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
        labels(np.array): n x 1 array with 0 / 1 adverse labels 
        ids(np.array): n x 1 array with dispatch ids
        featnames(list): the list of feature column names
    """

    feature_loader = FeatureLoader(start_date = start_date,
                                   end_date = end_date,
                                   table_name = table_name)

    # load the labels for the relevant dispatches
    dispatch_labels = feature_loader.dispatch_labeller(def_adverse)
    
    # select all the features which are set to True in the config file
    # NOTE: dict.items() is python 3 specific. for python 2 use dict.iteritems()
    features_to_use = [feat for feat, is_used in features.items() if is_used]

    features_df = feature_loader.load_all_features(features_to_use,
                                                   ids_to_use = dispatch_labels.dispatch_id,
                                                   feature_type = 'dispatch')

    # encode categorical features with dummy variables
    features_df_w_dummies = convert_categorical(features_df, features_to_use)
                                                   
    # join the labels and the features
    log.debug('... merging labels and features in memory')
    dataset = dispatch_labels.join(features_df_w_dummies, how='left', on='dispatch_id')
    log.debug('... dataset dataframe is {} bytes'.format(dataset.memory_usage().sum()))

    # NOTE: its important to run these inplace, otherwise you get a MemoryError (on a 15G RAM machine)
    dataset.set_index(["dispatch_id"], inplace=True)
    dataset.fillna(0, inplace=True)

    features = dataset.drop(["adverse_by_ourdef"], axis=1)
    labels = dataset["adverse_by_ourdef"].values
    ids = dataset.index.values
    feature_names = features.columns

    # make sure we return a non-zero number of labelled dispatches
    assert sum(labels) > 0, 'No dispatches were labelled adverse'

    log.debug("Dataset has {} rows and {} features".format(
       len(labels), len(feature_names)))

    return features, labels, ids, feature_names
