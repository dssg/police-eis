#!/usr/bin/env python

"""Script to build all of the tables defined in the CREATE-<table name>.sql files"""

import argparse
import os
import subprocess
import sys
import yaml

# read the credential file
parser = argparse.ArgumentParser()
parser.add_argument("-c", "--credentials", help="the credential file in yaml format")
parser.add_argument("-d", "--directory", help="the directory where the .sql files are stored", default=os.getcwd() )
args = parser.parse_args()
with open( args.credentials ) as infile:
	credentials = yaml.load( infile.read() )

# export credentials to the environment for this script and all sub-scripts.
os.environ["PGUSER"]     = credentials["PGUSER"]
os.environ["PGHOST"]     = credentials["PGHOST"]
os.environ["PGPASSWORD"] = credentials["PGPASSWORD"]
os.environ["PGDATABASE"] = credentials["PGDATABASE"]

# get the absolute path of the directory containing all the SQL files, and change to that directory.
directory = os.path.abspath( args.directory )
os.chdir(directory)

# the .sql files to run through psql
# NOTE: postgres schema creation statement is excluded here
sql_files = os.listdir(directory)
table_sql_files = [filename for filename in sql_files if ('CREATE' in filename and 'SCHEMA' not in filename)]
schema_sql_file = [filename for filename in sql_files if ('CREATE' in filename and 'SCHEMA' in filename)]

# run the CREATE-SCHEMA-<schema name>.sql files for the hubs through psql.
for sql_file in schema_sql_file:
    bash_cmd = """psql -f {}""".format(sql_file)
    print(bash_cmd)
    subprocess.call(bash_cmd, shell=True)

# Since the hubs will drop all dependent tables via CASCADE because of foreign primary key relations,
# drop them first.
#
# run each CREATE-<table name>.sql file through psql for all of the hubs.
for sql_file in [ filename for filename in table_sql_files if ('hub' in filename)]:
    bash_cmd = """psql -f {}""".format(sql_file)
    print(bash_cmd)
    subprocess.call(bash_cmd, shell=True)

# run each CREATE-<table name>.sql file through psql for all of the otehr tables.
for sql_file in [ filename for filename in table_sql_files if ('hub' not in filename)]:
    bash_cmd = """psql -f {}""".format(sql_file)
    print(bash_cmd)
    subprocess.call(bash_cmd, shell=True)
