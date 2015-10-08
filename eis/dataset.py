import numpy as np
import pdb
import pandas as pd
import yaml
import logging
import sys
import datetime

from eis import setup_environment, features


log = logging.getLogger(__name__)


class UnknownFeatureError(Exception):
    def __init__(self, feature):
        self.feature = feature

    def __str__(self):
        return "Unknown feature: {}".format(self.feature)


class FeatureLoader():

    def __init__(self, start_date, end_date):

        try:
            engine, dbconf = setup_environment.get_connection_from_profile()
            log.info("Connected to PostgreSQL database!")
        except IOError:
            log.exception("Failed to get database connection!")
            sys.exit(1)

        try:
            with open(dbconf, 'r') as f:
                config = yaml.load(f)
                log.info("Loaded experiment file")
        except:
            log.exception("Failed to get experiment configuration file!")

        self.con = engine.raw_connection()
        self.con.cursor().execute("SET SCHEMA '{}'".format(config['schema']))
        self.start_date = start_date
        self.end_date = end_date
        self.tables = config  # Dict of tables
        self.schema = config['schema']

    def labeller(self):
        """
        Load the IDs for a set of officers investigated between
        two dates and the outcomes

        Returns:
        labels: pandas dataframe with two columns:
        newid and adverse_by_ourdef
        """

        log.info("Loading labels...")
        query = ("SELECT newid, adverse_by_ourdef from {}.{} "
                 "WHERE dateoccured >= %(start_date)s "
                 "AND dateoccured <= %(end_date)s"
                 ).format(self.schema, self.tables["si_table"])

        labels = pd.read_sql(query, con=self.con, params={"start_date":
                             self.start_date, "end_date": self.end_date})
        return labels

    def loader(self, features_to_load):
        features = self.__read_feature_from_db(queries.sql[features_to_load],
                                               features_to_load,
                                               drop_duplicates=True)

        return features

    def __read_feature_from_db(self, query, features_to_load,
                               drop_duplicates=True):

        log.debug("Loading features for "
                  "%(start_date)s to %(end_date)s".format(
                        self.start_date, self.end_date))

        features = pd.read_sql(query, con=self.con,
                               params={"start_date": self.start_date,
                                       "end_date": self.end_date})

        if drop_duplicates:
            features = features.drop_duplicates(subset=["newid"])

        features = features.set_index(["newid"])
        # features = features[features_to_load]

        log.debug("... {} rows, {} features".format(len(features),
                                                    len(features.columns)))
        return features


def grab_data(features, start_date, end_date):
    """
    Function that defines the dataset.

    Inputs:
    -------
    features: dict containing which features to use
    start_date: date to start building features from
    end_date: date to stop building features until
    """

    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    data = FeatureLoader(start_date, end_date)

    for feature in features:
        if feature not in queries.sql:
            raise UnknownFeatureError(feature)

    officers = data.labeller()
    # officers.set_index(["newid"])

    dataset = officers
    for each_feat in features:
        if features[each_feat] == True:
            feature_df = data.loader(each_feat)
            dataset = dataset.join(feature_df, how='left', on='newid')

    dataset = dataset.reset_index()
    dataset = dataset.reindex(np.random.permutation(dataset.index))
    dataset = dataset.set_index(["newid"])

    dataset = dataset.dropna()

    labels = dataset["adverse_by_ourdef"].values
    feats = dataset.drop(["adverse_by_ourdef", "index"], axis=1)
    ids = dataset.index.values

    # Imputation will go here

    log.debug("Dataset has {} rows and {} features".format(
       len(labels), len(feats.columns)))

    return feats, labels, ids
