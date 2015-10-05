import numpy as np
import pdb
import pandas as pd
import yaml
import logging
import sys
import datetime

from eis import setup_environment


log = logging.getLogger(__name__)


class Features():

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

    def load_labels(self):
        """
        Load the IDs for a set of officers investigated between
        two dates and the outcomes

        Returns:
        labels: pandas dataframe with two columns:
        newid and adverse_by_ourdef
        """
        query = ("SELECT newid, adverse_by_ourdef from {}.{} "
                 "WHERE dateoccured >= %(start_date)s "
                 "AND dateoccured <= %(end_date)s"
                 ).format(self.schema, self.tables["si_table"])

        labels = pd.read_sql(query, con=self.con, params={"start_date":
                             self.start_date, "end_date": self.end_date})
        return labels


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

    mydata = Features(start_date, end_date)
    officers = mydata.load_labels()

    feats = "dummydummydummy"

    pdb.set_trace()
    return feats, officers
