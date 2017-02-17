import pdb
import copy
import threading
from itertools import product
import datetime
import logging

from . import officer
from . import setup_environment
from . import experiment
from .features import class_map
from .features import officers

log = logging.getLogger(__name__)

try:
    log.info("Connecting to database...")
    engine = setup_environment.get_database()
except:
    log.error('Could not connect to the database')

def create_labels_table(config, table_name):
    """Build the features table for the type of model (officer/dispatch) specified in the config file"""

    if config['unit'] == 'officer':
        create_officer_labels_table(config, table_name)
    # TODO:
    #if config['unit'] == 'dispatch':
    #    create_dispatch_labels_table(config, table_name)


def populate_labels_table(config, table_name):
    """Calculate values for all features which are set to True (in the config file)
    for the appropriate run type (officer/dispatch)
    """

    if config['unit'] == 'officer':
        populate_officer_labels_table(config, table_name)
    # TODO:
    #if config['unit'] == 'dispatch':
    #    populate_dispatch_labels_table(config, table_name)

def create_officer_labels_table(config, table_name="officer_labels"):
    """ Creates a features.table_name table within the features schema """


    # drop the old features table
    log.info("Dropping the old officer labels table: {}".format(table_name))
    engine.execute("DROP TABLE IF EXISTS features.{}".format(table_name) )

    # use the appropriate id column, depending on feature types (officer / dispatch)
    id_column = '{}_id'.format(config['unit'])

    # Create and execute a query to create a table with a column for each of the labels.
    log.info("Creating new officer feature table: {}...".format(table_name))
    create_query = (    "CREATE TABLE features.{} ( "
                        "   {}                   int, "
                        "   event_datetime       timestamp, "
                        "   outcome_datetime     timestamp, "
                        "   outcome              text); "
                        .format(
                            table_name,
                            id_column))

    engine.execute(create_query)
    query_index = ("CREATE INDEX ON features.{} (outcome_datetime, officer_id)".format(table_name))
    engine.execute(query_index)

def populate_officer_labels_table(config, table_name):
    """ Populates officer labels table in the database using staging.incidents.
        The table consists on four columns:
           - officer_id
           - event_datetime
           - grouped_incident_type_code
           - final_ruling_code
     """
    query_sustained = "final_ruling_code in ( 1, 4, 5 ) "
    query_unknown_ruling = "final_ruling_code in (0) or final_ruling_code is null"
    #query_all       = "number_of_allegations > 0 "
    query_major     = "grouped_incident_type_code in ( 0, 2, 3, 4, 8, 9, 10, 11, 17, 20 ) "
    query_minor     = "grouped_incident_type_code in ( 1, 6, 16, 18, 12, 7, 14 ) "
    query_force     = "grouped_incident_type_code = 20 "
    query_unknown   = "grouped_incident_type_code = 19 "

    labels_rules = {
         "ForceAllegations": {"query": query_force, "datetime": 'report_date'},
         "SustainedForceAllegations": {"query": query_force + " AND " + query_sustained, "datetime": 'date_of_judgment'},
         #"all_allegations": query_all,
         "SustainedAllegations": {"query": query_sustained, "datetime": 'date_of_judgment'},
         "MajorAllegations": {"query": query_major, "datetime": 'report_date'},
         "SustainedMajorAllegations": {"query": query_major + " AND " + query_sustained, "datetime": 'date_of_judgment'},
         "MinorAllegations": {"query": query_minor, "datetime": 'report_date'},
         "SustainedMinorAllegations": {"query": query_minor + " AND " + query_sustained, "datetime": 'date_of_judgment'},
         "UnknownAllegations": {"query": query_unknown, "datetime": 'report_date'},
         "SustainedUnknownAllegation": {"query": query_unknown + " AND " + query_sustained, "datetime": 'date_of_judgment'},
                 }
 

    query_list = []
    for label, label_rule in labels_rules.items():
        query_list.append("SELECT officer_id, "
                        "        {0} as outcome_datetime, "
                        "        event_datetime, "
                        "       '{1}' as outcome "
                        "FROM staging.incidents "
                        "WHERE {2} ".format(label_rule['datetime'],
                                            label, 
                                            label_rule['query']))

    query_join = " UNION ".join(query_list)
 
    insert_query = ( "INSERT INTO features.{0}  "
              "         ( officer_id, "
              "           event_datetime, "
              "           outcome_datetime, "
              "           outcome ) "
              "         {1}  "
              .format(table_name, query_join))

    engine.execute(insert_query)          
