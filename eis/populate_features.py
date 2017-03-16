import pdb
import copy
import threading
from itertools import product
import datetime
import logging

from . import officer
from . import setup_environment
from . import utils
from .features import class_map
from .features import officers_collate

log = logging.getLogger(__name__)


def populate_features_table(config, table_name, schema):
    """Calculate values for all features which are set to True (in the config file) 
    for the appropriate run type (officer/dispatch)
    """
    engine = setup_environment.get_database()
    if config['unit'] == 'officer':
        populate_officer_features_table(config, table_name, schema, engine)
    if config['unit'] == 'dispatch':
        populate_dispatch_features_table(config, table_name, engine)


def populate_dispatch_features_table(config, table_name, engine):
    """Calculate all the feature values and store them in the features table in the database"""

    # Get a list of all the features that are set to true.
    feature_list = [feat for feat, is_set_true in config['dispatch_features'].items() if is_set_true]
    num_features = len(feature_list)

    # make sure we have at least 1 feature
    assert num_features > 0, 'List of features to build is empty'

    feature_threads = []

    # run the build_and_insert of a set of features
    def run_thread(feature_sublist, engine):

        db_conn = engine.connect()

        for feature_name in feature_sublist:
            log.debug('... building feature {}'.format(feature_name))

            feature_obj = class_map.lookup(feature_name,
                                           unit='dispatch',
                                           from_date=config['raw_data_from_date'],
                                           to_date=config['raw_data_to_date'],
                                           fake_today=datetime.datetime.today(),
                                           table_name=table_name)
            feature_obj.build_and_insert(db_conn)

        db_conn.close()

    def chunks(l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i:i + n]

    # build each feature and store it in its own table in features_prejoin
    # start a new thread for each set of 5 features
    for feature_sublist in chunks(feature_list, 5):
        t = threading.Thread(target=run_thread, args=(feature_sublist, engine,))
        feature_threads.append(t)
        t.start()

    # join each thread and wait for it to be done to make sure we're done building them all
    # before we move on to joining them
    for i, thread in enumerate(feature_threads):
        log.debug('Waiting for feature thread: {}/{})'.format(i, (num_features / 5)))
        thread.join()

    # join each single-feature to the main table one at a time
    for i, feature_name in enumerate(feature_list):
        log.debug("Adding feature {}/{} ({})".format(i, len(feature_list), feature_name))

        update_query = ("UPDATE features.{table_name} AS feature_table "
                        "SET {feature} = prejoin_table.{feature} "
                        "FROM features_prejoin.{feature} AS prejoin_table "
                        "WHERE feature_table.dispatch_id = prejoin_table.dispatch_id "
                        .format(table_name=table_name,
                                feature=feature_name))

        engine.execute(update_query)


def join_feature_table(engine, list_prefixes, schema, features_table_name):
    """
    This function joins the block tables into the features_table_name
    using the prefix of the aggregated tables specified in each class

    :param engine: engine to connect to db
    :param list list_prefixes: list of prefixes specified in each block class
    :param str schema: name of schema where collate table are stored
    :param str features_table_name: name of the table to create in the features schema
    """
    table_names = ['{}_aggregation'.format(prefix) for prefix in list_prefixes]

    # seperate the tables by block that have a date column or not
    table_names_no_date = [x for x in table_names if 'ND' in x]
    table_names_with_date = [x for x in table_names if x not in set(table_names_no_date)]

    query = ""
    if len(table_names) > 0:
        if table_names_with_date:
            query = """ select * from {}."{}" """.format(schema, table_names_with_date[0])
            table_names_with_date = table_names_with_date[1:]
        for table_name in table_names_with_date:
            query += """ full outer join {}."{}"  using (officer_id, as_of_date)""".format(schema, table_name)

        # check if in the first loop above a table was added
        if len(query) == 0:
            query = """ select * from {}."{}" """.format(schema, table_names_no_date[0])
            table_names_no_date = table_names_no_date[1:]

        for table_name in table_names_no_date:
                query += """ full outer join {}."{}"  using (officer_id)""".format(schema, table_name)

        drop_table_query = """DROP TABLE IF EXISTS features."{}";""".format(features_table_name)
        engine.execute(drop_table_query)

        create_table_query = """CREATE TABLE features."{0}" as ({1});""".format(features_table_name,
                                                                                query)
        engine.execute(create_table_query)

        create_as_of_date_index = """CREATE INDEX on features."{0}" (as_of_date);  """.format(features_table_name)
        engine.execute(create_as_of_date_index)

        create_officer_date_index = """CREATE INDEX on features."{0}" (as_of_date, officer_id);  """.format(features_table_name)
        engine.execute(create_officer_date_index)


def populate_officer_features_table(config, table_name, schema, engine):
    """
     Calculate all the feature values and store them in the features table in the database
     using collate method that creates a table of feature for each block and stores them in a 
     given schema. Then joins all the tables into a new table (table_name) on the features schema
     
     Args:
        config: Python dict read in from YAML config file containing
                user-supplied details of the experiments to be run
        table_name: table name for storing all the features in the features schema
        schema: schama name for storing collate tables
     """
    temporal_info = config['temporal_info'].copy()
    # get the list of fake todays specified by the config file
    as_of_dates = utils.generate_feature_dates(temporal_info)
    log.debug(as_of_dates)

    list_prefixes = []
    # get a list of all features that are set to true.
    for block_name in config["officer_features"]:
        log.debug('block_name: {}'.format(block_name))
        block = config['feature_blocks'][block_name]
        feature_list = [key for key in block if block[key] == True]

        ## Need to find a way of calling the class given the block_name
        block_class = class_map.lookup_block(block_name,
                                             module=officers_collate,
                                             lookback_durations=temporal_info['timegated_feature_lookback_duration'],
                                             n_cpus=config['n_cpus'])

        # Build collate tables and returns table name
        block_class.build_collate(engine, as_of_dates, feature_list, schema)
        list_prefixes.extend(block_class.prefix)

    # Join all tables into one
    log.debug(list_prefixes)
    join_feature_table(engine, list_prefixes, schema, table_name)
