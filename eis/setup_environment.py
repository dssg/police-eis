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


def get_database(production=None):
    try:
        engine = get_connection_from_profile(production=production)
        log.info("Connected to PostgreSQL database!")
    except IOError:
        log.exception("Failed to get database connection!")
        return None, 'fail'

    return engine


def get_connection_from_profile(config_file_name="default_profile.yaml", production=None):
    """
    Sets up database connection from config file.

    Input:
    config_file_name: File containing PGHOST, PGUSER,
                      PGPASSWORD, PGDATABASE, PGPORT, which are the
                      credentials for the PostgreSQL database
    """

    with open(config_file_name, 'r') as f:
        vals = yaml.load(f)

    if not ('PGHOST' in vals.keys() and
            'PGUSER' in vals.keys() and
            'PGPASSWORD' in vals.keys() and
            'PGDATABASE' in vals.keys() and
            'PGPORT' in vals.keys()):
        raise Exception('Bad config file: ' + config_file_name)

    return get_engine(vals['PGDATABASE'], vals['PGUSER'],
                      vals['PGHOST'], vals['PGPORT'],
                      vals['PGPASSWORD'], production)


def get_engine(db, user, host, port, passwd, production=None):
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
    if not production:
        engine = create_engine(url, poolclass=NullPool)
    else:
        engine = create_engine(
                url,
                execution_options={'schema_translate_map': {
                    'results': 'production'
                }},
                poolclass=NullPool
        )

    return engine

