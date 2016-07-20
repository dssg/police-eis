import pdb
import copy
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

def drop_and_load_features_schema():
    """ If the features schema exists, DROP and CREATE.  If it doesn't, CREATE """

    # Create the schema.
    log.info("Connecting to database...")
    engine, db_config = setup_environment.get_database()
    log.info("Dropping the old features schema...")
    engine.execute("DROP SCHEMA IF EXISTS features CASCADE") # Note: this should be confirmed by the user before dropping the features.
    engine.execute("CREATE SCHEMA features") 

def create_features_table(config, table_name="features" ):
    """ Creates a features.table_name table within the features schema """

    log.info("Re-building features...")

    # Drop the table and rebuild.
    log.info("Connecting to database...")
    engine, db_config = setup_environment.get_database()
    db_conn = engine.raw_connection()
    log.info("Dropping the old feature table...")
    db_conn.cursor().execute("DROP TABLE IF EXISTS features.{}".format(table_name) )

    # Get a list of all the features that are set to true.
    feature_classes = config["features"]
    feature_list  = []
    feature_value = []
    for classkey in feature_classes:
        for feature in feature_classes[classkey]:
            if config["features"][classkey][feature]:
                feature_list.extend([feature])
                feature_value.extend([True])

    print( feature_list )
#        feature_list.extend( feature_classes["arrests"].keys() )
#        feature_value.extend( feature_classes["arrests"].values() )

    # Split the height_weight feature into two features.
    if "height_weight" in feature_list:
        feature_list.extend(["height","weight"])
        feature_list.remove("height_weight")


    # Create and execute a query to create a table with a column for each of the features.
    log.info("Creating new feature table: {}...".format(table_name))
    create_query = "CREATE TABLE features.{} ( \n".format(table_name)
    create_query += "\t officer_id \t int,\n"
    create_query += "\t created_on \t timestamp,\n"
    create_query += "\t fake_today \t timestamp,\n"
    feature_query = [ "\t {} \t numeric \n".format(x) for x in feature_list ]
    create_query += ', '.join(feature_query)
    create_query += ");"
    engine.execute( create_query )

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
        engine.execute( officer_id_query )

def populate_features_table(config):
    """Calculate all the feature values and store them in the features table in the database"""

    # connect to the database
    log.debug("Connecting to database...")
    engine, db_config = setup_environment.get_database()

    # get the list of fake todays specified by the config file
    temporal_info = experiment.generate_time_info(config)

    # using a set comprehension to remove duplicates, bc temporal_info gives multiple time windows
    # which we don't care about here
    fake_todays = {time_dict['fake_today'] for time_dict in temporal_info}

    # loop through the feature groups
    feature_groups = config["features"]
    for feature_group in feature_groups:

        # select each feature in the group individually if it is active in the configuration file.
        active_features = [feature_name for feature_name in config["features"][feature_group].keys() if config["features"][feature_group][feature_name] ]
        for feature_name in active_features:

            # loop over each fake today
            for fake_today in fake_todays:
    
                feature_class = class_map.lookup(feature_name, 
                                                 fake_today=datetime.datetime.strptime(fake_today, "%d%b%Y" ),
                                                 table_name="features" )
                log.debug('Calculated and inserted feature {} for fake_today {}'
                            .format(feature_class.feature_name, fake_today))
                feature_class.build_and_insert( engine )

