#!/usr/bin/env python
import os
import yaml
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import logging

log = logging.getLogger(__name__)


def get_experiment_config(exp_config_file_name='experiment.yaml'):
    """Get the experiment configuration variables from the config file

    Args:
        exp_config_file_name(str): the path to the experiment config file
    """
    try:
        with open(exp_config_file_name, 'r') as f:
            config = yaml.load(f)
            log.debug("Loaded experiment configuration file")
    except:
        log.exception("Failed to get experiment configuration file!")
        return None

    return config


def get_database():
    try:
        engine = get_connection_from_profile()
        log.info("Connected to PostgreSQL database!")
        return engine
    except IOError:
        log.exception("Failed to get database connection!")
        raise


def get_connection_from_profile(config_file_name="default_profile.yaml"):
    """
    Sets up database connection from config file. If the config file is not available,
    it tries to load the Postgres credentials from environmental variables instead.

    Input:
    config_file_name: File containing PGHOST, PGUSER,
                      PGPASSWORD, PGDATABASE, PGPORT, which are the
                      credentials for the PostgreSQL database
    """

    try:
        with open(config_file_name, 'r') as f:
            vals = yaml.load(f)
    except IOError:
        log.warning("YAML file %s not available, attempting to load "
                    "Postgres variables from environment instead." 
                    %config_file_name)
        vals = os.environ

    if not ('PGHOST' in vals.keys() and
            'PGUSER' in vals.keys() and
            'PGPASSWORD' in vals.keys() and
            'PGDATABASE' in vals.keys() and
            'PGPORT' in vals.keys()):
        log.error('Cannot find the necessary Postgres credential env variables')
        raise Exception('Bad Postgres configuration!')

    return get_engine(vals['PGDATABASE'], vals['PGUSER'],
                      vals['PGHOST'], vals['PGPORT'],
                      vals['PGPASSWORD'])


def get_engine(db, user, host, port, passwd):
    """
    Get SQLalchemy engine using credentials.

    Input:
    db: database name
    user: Username
    host: Hostname of the database server
    port: Port number
    passwd: Password for the database
    """

    url = 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
        user=user, passwd=passwd, host=host, port=port, db=db)
    engine = create_engine(url, poolclass=NullPool)
    return engine
