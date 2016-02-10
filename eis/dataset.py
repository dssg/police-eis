import numpy as np
import pdb
import pandas as pd
import yaml
import logging
import sys
import datetime
import json

from eis import setup_environment
from eis.features import class_map

log = logging.getLogger(__name__)
engine, config = setup_environment.get_database()
con = engine.raw_connection()
change_schema(config['schema'])


def change_schema(schema):
    con.cursor().execute("SET SCHEMA '{}'".format(schema))
    return None


def enter_into_db(timestamp, config, auc):
    query = ("INSERT INTO models.full (id_timestamp, config, auc)"
             "VALUES ('{}', '{}', {})".format(timestamp, json.dumps(config), auc))
    con.cursor().execute(query)
    return None


def format_officer_ids(ids):
    formatted = ["{}".format(each_id) for each_id in ids]
    formatted = ", ".join(formatted)
    return formatted


def get_baseline(ids, start_date, end_date):
    """
    Gets EIS baseline - whether or not an officer is flagged 
    by the EIS at any point in the labelling window. 

    Inputs:
    ids: officer ids
    start_date: beginning of training period
    end_date: end of training period

    Returns: dataframe with ids of those officers flagged by EIS
    """

    flagged_officers = ("select distinct newid from {} "
                        "WHERE datecreated >= '{}'::date "
                        "AND datecreated <='{}'::date").format(
                            config["eis_table"],
                            start_date, end_date)

    df_eis_baseline = pd.read_sql(flagged_officers, con=con) 

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

    intervened_officers = ("select distinct newid from {} "
                        "WHERE intervention != 'No Intervention Required' "
                        "AND datecreated >= '{}'::date "
                        "AND datecreated <='{}'::date "
                        "AND newid in ({}) ").format(
                            config["eis_table"],
                            start_date, end_date,
                            format_officer_ids(ids))

    no_intervention_officers = ("select distinct newid from {} "
                        "WHERE intervention = 'No Intervention Required' "
                        "AND datecreated >= '{}'::date "
                        "AND datecreated <='{}'::date "
                        "AND newid in ({}) ").format(
                            config["eis_table"],
                            start_date, end_date,
                            format_officer_ids(ids))

    df_intervention = pd.read_sql(intervened_officers, con=con)
    df_no_intervention = pd.read_sql(no_intervention_officers, con=con)
    df_intervention["intervention"] = 1
    df_action = df_intervention.merge(df_no_intervention, how='right', on='newid')
    df_action = df_action.fillna(0)

    return df_action


def get_labels_for_ids(ids, start_date, end_date):
    qinvest = ("SELECT newid, count(adverse_by_ourdef) from {} "
                  "WHERE dateoccured >= '{}'::date "
                  "AND dateoccured <= '{}'::date "
                  "AND newid in ({}) "
                  "group by newid "
                  ).format(config["si_table"],
                           start_date, end_date,
                           format_officer_ids(ids))

    qadverse = ("SELECT newid, count(adverse_by_ourdef) from {} "
                    "WHERE adverse_by_ourdef = 1 "
                    "AND dateoccured >= '{}'::date "
                    "AND dateoccured <= '{}'::date "
                    "AND newid in ({}) "
                    "group by newid "
                    ).format(config["si_table"],
                             start_date, end_date,
                             format_officer_ids(ids))

    invest = pd.read_sql(qinvest, con=con)
    adverse = pd.read_sql(qadverse, con=con)
    adverse["adverse_by_ourdef"] = 1
    adverse = adverse.drop(["count"], axis=1)
    invest = invest.drop(["count"], axis=1)
    outcomes = adverse.merge(invest, how='right', on='newid')
    outcomes = outcomes.fillna(0)

    return outcomes


def imputation_zero(df, ids):
    fulldf = pd.DataFrame(ids, columns=["newid"] + [df.columns[0]]) 
    df["newid"] = df.index
    newdf = df.merge(fulldf, how="right", on="newid")
    newdf = newdf.fillna(0)
    newdf[df.columns[0]] = newdf[df.columns[0] + "_x"] + newdf[df.columns[0] + "_y"] 
    newdf = newdf.drop([df.columns[0] + "_x", df.columns[0] + "_y"], axis=1)
    newdf = newdf.set_index("newid")
    return newdf


def imputation_mean(df, featurenames):
    try:
        newdf = df.set_index("newid")
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


def convert_categorical(df):
    """
    this function generates features from a nominal feature

    Inputs:
    df: a dataframe with two columns: id, and a feature which is 1
    of n categories

    Outputs:
    df: a dataframe with 1 + n columns: id and boolean features that are
    "category n" or not
    """

    onecol = df.columns[0]
    categories = pd.unique(df[onecol])

    # Remove empty fields
    # Replace Nones or empty fields with NaNs?
    categories = [x for x in categories if x is not None]
    try:
        categories.remove(' ')
    except:
        pass

    # Get rid of tricksy unicode strings
    categories = [str(x) for x in categories]

    # Get rid of capitalization differences
    categories = list(set([str.lower(x) for x in categories]))

    # Set up new features
    featnames = []
    for i in range(len(categories)):
        if type(categories[i]) is str:
            newfeatstr = 'is_' + categories[i]
            featnames.append(newfeatstr)
            df[newfeatstr] = df[onecol] == categories[i]

    df = df.drop(onecol, axis=1)
    return df.astype(int), list(df.columns)


class FeatureLoader():

    def __init__(self, start_date, end_date, fake_today):

        self.start_date = start_date
        self.end_date = end_date
        self.fake_today = fake_today
        self.tables = config  # Dict of tables
        self.schema = config['schema']

    def officer_labeller(self, accidents, noinvest):
        """
        Load the IDs for a set of officers investigated between
        two dates and the outcomes

        Inputs:
        accidents: Bool representing if accidents should be included
        noinvest: Bool representing how officers with no investigations 
        should be treated - True means they are included as "0", False means they
        are excluded

        Returns:
        labels: pandas dataframe with two columns:
        newid and adverse_by_ourdef
        """

        log.info("Loading labels...")

        if noinvest == True:
            qinvest = "SELECT newid from {}".format(self.tables["active_officers"])
        else:
            qinvest = ("SELECT newid, count(adverse_by_ourdef) from {} "
                      "WHERE dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by newid "
                      ).format(self.tables["si_table"],
                               self.start_date, self.end_date)

        if accidents == False:
            qadverse = ("SELECT newid, count(adverse_by_ourdef) from {} "
                        "WHERE adverse_by_ourdef = 1 "
                        "AND eventtype != 'Accident' "
                        "AND dateoccured >= '{}'::date "
                        "AND dateoccured <= '{}'::date "
                        "group by newid "
                        ).format(self.tables["si_table"],
                                 self.start_date, self.end_date)
        else:     
            qadverse = ("SELECT newid, count(adverse_by_ourdef) from {} "
                        "WHERE adverse_by_ourdef = 1 "
                        "AND dateoccured >= '{}'::date "
                        "AND dateoccured <= '{}'::date "
                        "group by newid "
                        ).format(self.tables["si_table"],
                                 self.start_date, self.end_date)

        invest = pd.read_sql(qinvest, con=con)
        adverse = pd.read_sql(qadverse, con=con)
        adverse["adverse_by_ourdef"] = 1
        adverse = adverse.drop(["count"], axis=1)
        if noinvest == False:
            invest = invest.drop(["count"], axis=1)
        outcomes = adverse.merge(invest, how='right', on='newid')
        outcomes = outcomes.fillna(0)

        # labels = labels.set_index(["newid"])

        return outcomes

    def dispatch_labeller(self):
        """
        Load the dispatch events investigated between
        two dates and the outcomes

        Returns:
        labels: pandas dataframe with two columns:
        newid, adverse_by_ourdef, dateoccured
        """

        log.info("Loading labels...")

        # These are the objects flagged as ones and twos
        query = ("SELECT newid, adverse_by_ourdef, "
                 "dateoccured from {}.{} "
                 "WHERE dateoccured >= '{}'::date "
                 "AND dateoccured <= '{}'::date"
                 ).format(self.schema, self.tables["si_table"],
                          self.start_date, self.end_date)

        labels = pd.read_sql(query, con=con)

        # Now also label those not sampled
        # Grab all dispatch events and filter out the ones
        # already included

        pdb.set_trace()

        return labels

    def loader(self, features_to_load, ids):
        kwargs = {"time_bound": self.fake_today,
                  "feat_time_window": 0}
        feature = class_map.lookup(features_to_load, **kwargs)

        if type(feature.query) == str:
            results = self.__read_feature_from_db(feature.query,
                                                  features_to_load,
                                                  drop_duplicates=True)
            featurenames = feature.name_of_features
        elif type(feature.query) == list:
            featurenames = []
            results = pd.DataFrame()
            for each_query in feature.query:
                ea_resu = self.__read_feature_from_db(each_query,
                                                      features_to_load,
                                                      drop_duplicates=True)
                featurenames = featurenames + feature.name_of_features

                if len(results) == 0:
                    results = ea_resu
                    results['newid'] = ea_resu.index
                else:   
                    results = results.join(ea_resu, how='left', on='newid')

        if feature.type_of_features == "categorical":
            results, featurenames = convert_categorical(results)
        elif feature.type_of_features == "series":
            results, featurenames = convert_series(results)

        if feature.type_of_imputation == "zero":
                results = imputation_zero(results, ids)
        elif feature.type_of_imputation == "mean":
                results, featurenames = imputation_mean(results, featurenames)

        return results, featurenames

    def __read_feature_from_db(self, query, features_to_load,
                               drop_duplicates=True):

        log.debug("Loading features for events from "
                  "%(start_date)s to %(end_date)s".format(
                        self.start_date, self.end_date))

        results = pd.read_sql(query, con=con)

        if drop_duplicates:
            results = results.drop_duplicates(subset=["newid"])

        results = results.set_index(["newid"])
        # features = features[features_to_load]

        log.debug("... {} rows, {} features".format(len(results),
                                                    len(results.columns)))
        return results


def grab_officer_data(features, start_date, end_date, time_bound, accidents,
                      noinvest):
    """
    Function that defines the dataset.

    Inputs:
    -------
    features: dict containing which features to use
    start_date: start date for selecting officers
    end_date: end date for selecting officers
    time_bound: build features with respect to this date
    accidents: if True, include accidents, if False exclude accidents
    noinvest: if True, then all officers included those not investigated
    by IA should be labelled, if False then only those investigated will
    be labelled
    """

    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    data = FeatureLoader(start_date, end_date, time_bound)

    officers = data.officer_labeller(accidents, noinvest)
    # officers.set_index(["newid"])

    dataset = officers
    featnames = []
    for each_feat in features:
        if features[each_feat] == True:
            feature_df, names = data.loader(each_feat,
                                            dataset["newid"])
            log.info("Loaded feature {} with {} rows".format(
                featnames, len(feature_df)))
            featnames = list(featnames) + list(names)
            dataset = dataset.join(feature_df, how='left', on='newid')

    dataset = dataset.reset_index()
    dataset = dataset.reindex(np.random.permutation(dataset.index))
    dataset = dataset.set_index(["newid"])

    dataset = dataset.dropna()

    labels = dataset["adverse_by_ourdef"].values
    feats = dataset.drop(["adverse_by_ourdef", "index"], axis=1)
    ids = dataset.index.values

    log.debug("Dataset has {} rows and {} features".format(
       len(labels), len(feats.columns)))

    return feats, labels, ids, featnames
