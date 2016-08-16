import pdb
import copy
import threading
from itertools import product
import datetime
import logging
from IPython.core.debugger import Tracer

from . import officer
from . import setup_environment
from . import experiment
from .features import class_map
from .features import officers

log = logging.getLogger(__name__)

try:
    log.info("Connecting to database...")
    engine, _ = setup_environment.get_database()
except:
    log.error('Could not connect to the database')
    
def create_features_table(config, table_name):
    """Build the features table for the type of model (officer/dispatch) specified in the config file"""

    if config['unit'] == 'officer':
        create_officer_features_table(config, table_name)
    if config['unit'] == 'dispatch':
        create_dispatch_features_table(config, table_name)


def populate_features_table(config, table_name):
    """Calculate values for all features which are set to True (in the config file) 
    for the appropriate run type (officer/dispatch)
    """

    if config['unit'] == 'officer':
        populate_officer_features_table(config, table_name)
    if config['unit'] == 'dispatch':
        populate_dispatch_features_table(config, table_name)


def create_officer_features_table(config, table_name="officer_features"):
    """ Creates a features.table_name table within the features schema """

    # drop the old features table
    log.info("Dropping the old officer feature table: {}".format(table_name))
    engine.execute("DROP TABLE IF EXISTS features.{}".format(table_name) )

    # get a list of table column names.
    column_names = officer.get_officer_features_table_columns( config )

    # Get a list of all the features that are set to true.
    features = config["officer_features"]
    feature_list = [ key for key in features if features[key] == True ]
    feature_value = [True]*len(feature_list)

    # make sure we have at least 1 feature
    assert len(feature_list) > 0, 'List of features to build is empty'

    # use the appropriate id column, depending on feature types (officer / dispatch)
    id_column = '{}_id'.format(config['unit'])

    # Create and execute a query to create a table with a column for each of the features.
    log.info("Creating new officer feature table: {}...".format(table_name))
    create_query = (    "CREATE UNLOGGED TABLE features.{} ( "
                        "   {}              int, "
                        "   created_on      timestamp, "
                        "   fake_today      timestamp, "
                        .format(
                            table_name,
                            id_column))

    # create a column for all the features we'll generate.
    feature_query = ', '.join(["{} numeric ".format(x) for x in column_names])

    final_query = create_query + feature_query + ");"

    engine.execute(final_query)

    # Get the list of fake_todays.
    temporal_info = experiment.generate_time_info(config)
    fake_todays = {time_dict['fake_today'] for time_dict in temporal_info}

    # Populate the features table with officer_id.
    log.info("Populating feature table {} with officer ids and fake todays...".format(table_name))
    time_format = "%Y-%m-%d %X"
    for fake_today in fake_todays:
        fake_today = datetime.datetime.strptime(fake_today, '%d%b%Y') 
        fake_today.strftime(time_format)
        officer_id_query = ( "INSERT INTO features.{} (officer_id,fake_today) "
                             "SELECT staging.officers_hub.officer_id, '{}'::date "
                             "FROM staging.officers_hub").format(table_name,fake_today)
        engine.execute(officer_id_query)


def create_dispatch_features_table(config, table_name="dispatch_features"):

    # drop the old features table
    log.info("Dropping the old dispatch feature table: {}".format(table_name))
    engine.execute("DROP TABLE IF EXISTS features.{}".format(table_name))

    # Create and execute a query to create a table with a column for each of the features.
    log.info("Creating new dispatch feature table: {}".format(table_name))

    create_query = (    "CREATE TABLE features.{} "
                        "AS SELECT  "
                        "   events_hub.dispatch_id as dispatch_id, "
                        "   MIN(events_hub.event_datetime) as fake_today "
                        "FROM staging.events_hub "
                        "WHERE event_datetime between '{}' and '{}' "
                        "AND dispatch_id IS NOT NULL "
                        "AND event_type_code = 5 "
                        "GROUP BY dispatch_id "
                        .format(
                            table_name,
                            config['raw_data_from_date'],
                            config['raw_data_to_date']))

    engine.execute(create_query)

    # Create an index on the dispatch_id column to speed up joins
    log.info("Creating index on dispatch_id column")
    indexing_query = ("CREATE INDEX ON features.{} (dispatch_id)").format(table_name)
    engine.execute(indexing_query)

def populate_dispatch_features_table(config, table_name):
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
            log.debug('Building feature {}'.format(feature_name))

            feature_obj = class_map.lookup(feature_name, 
                                        from_date = config['raw_data_from_date'],
                                        to_date = config['raw_data_to_date'],
                                        fake_today = datetime.datetime.today(),
                                        table_name = table_name)
            feature_obj.build_and_insert(db_conn)

        db_conn.close()

    def chunks(l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i:i + n]

    # build each feature and store it in its own table in features_prejoin
    # start a new thread for each feature
    for feature_sublist in chunks(feature_list, 10):

        t = threading.Thread(target=run_thread, args=(feature_sublist, engine,))
        feature_threads.append(t)
        t.start()

    # join each thread and wait for it to be done to make sure we're done building them all
    # before we move on to joining them
    for i, thread in enumerate(feature_threads):
        log.debug('Waiting for feature thread: {}/{})'.format(i, num_features))
        thread.join()

    # move the table w just dispatch_ids to feature_table_temp
    temp_table_name = table_name + "_temp"
    rename_query = "ALTER TABLE features.{} RENAME TO {}".format(table_name, temp_table_name)
    engine.execute(rename_query)

    # join all the 1-column feature tables in the features_prejoin folder/schema to the main features table
    join_query = (  "CREATE TABLE features.{} "
                    "AS SELECT "
                    "   master.dispatch_id, "
                    "   master.fake_today, "
                    "   {} "
                    "FROM features.{} as master "
                    .format(
                        table_name,
                        ", ".join(feature_list),
                        temp_table_name))

    for feature_name in feature_list:
        join_query +=  ("LEFT JOIN features_prejoin.{} as {} "
                        "   ON master.dispatch_id = {}.dispatch_id "
                        .format(feature_name,
                                feature_name,
                                feature_name))

    # run the giant joining query
    log.debug('Joining features table')
    engine.execute(join_query)

    # drop the old features table
    drop_query = "DROP TABLE features.{}".format(temp_table_name)
    engine.execute(drop_query)


def populate_officer_features_table(config, table_name):
    """Calculate all the feature values and store them in the features table in the database"""

    # get the list of fake todays specified by the config file
    temporal_info = experiment.generate_time_info(config)

    # using a set comprehension to remove duplicates, bc temporal_info gives multiple time windows
    # which we don't care about here
    fake_todays = {time_dict['fake_today'] for time_dict in temporal_info}

    # get a list of all features that are set to true.
    active_features = [ key for key in config["officer_features"] if config["officer_features"][key] == True ] 

    # loop over all fake todays, populating the active features for each.
    for feature_name in active_features:
        for fake_today in fake_todays:
            feature_class = class_map.lookup(feature_name, 
                                             fake_today=datetime.datetime.strptime(fake_today, "%d%b%Y" ),
                                             table_name=table_name, 
                                             lookback_durations=config["timegated_feature_lookback_duration"])
            feature_class.build_and_insert(engine)
            log.debug('Calculated and inserted feature {} for fake_today {}'
                        .format(feature_class.feature_name, fake_today))
