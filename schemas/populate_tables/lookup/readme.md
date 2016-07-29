# Populating the Lookup Tables

Given that the lookup tables are relatively small, static tables we have written a simple Python script (`populate_lookup_tables.py`) that takes care of their creation and population (note that this script relies upon the empty lookup tables already existing).

It takes a .yaml file containing information about the lookup table layouts and values (`lookup_tables.yaml`) and a database connection configuration file (also a .yaml file) as inputs.

Usage is as follows from the command line (in this case run from the same directory as the Python script and the lookup tables .yaml file):

	python populate_lookup_tables.py --credentials=<path/to/credentials_file> --tablefile=lookup_tables.yaml
	
The credentials file (e.g. cmpd_credentials.yaml) should be added to the .gitignore file, or stored in a completely separate folder. It should contain the following lines:

	PGHOST: postgres.dssg.io
	PGDATABASE: cmpd_2015
	PGUSER: cmpd
	PGPASSWORD: <password for user cmpd>

