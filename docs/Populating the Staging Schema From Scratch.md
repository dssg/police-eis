# How to Populate the Staging Schema From Scratch

1. Get all the data into one place! For what follows, all of the tables we received from Charlotte in both 2015 and 2016 were combined and placed in a single schema which we called `cmpd_merged`.

  **TODO - where is this documented at the moment?**

2. Make sure you have the Postgres environment variables set (so that `psql` commands can be run without faff:  

	 	export PGHOST=postgres.dssg.io
	 	export PGDATABASE=cmpd_2015
	 	export PGUSER=cmpd
	 	export PGPASSWORD=<password>
	 	
3. Create a schema called 'staging': 

		psql "CREATE SCHEMA staging"

4. Run the create table scripts. Navigate to `schemas/create_tables` and run the following command (note that the `default_profile.yaml` requires the Postgres credentials to be filled out:  

		python build_staging_schema.py --credentials=default_profile.yaml
		
   If you have chosen a schema names other than 'staging' you will need to replace the schema name in the raw files... this can be done with a `sed` command (NB this modifies the files in-place):
   
   		sed -i 's/staging\./<different_schema_name>\./g' *.sql 
	 	
5. The next stage is to run the pre-staging scripts, which add some stored procedures and create some indexed tables that make loading data into the staging schema considerably faster. Navigate to the private repo (`police-eis-private`), and run both:

		psql -f schemas/prestaging/cmpd/raw_table_modifications.sql
		psql -f schemas/stored_procedures/cmpd/lookup_codes_stored_procedures.sql
		
 The first of these commands will take a long time to run (most likely several hours).

6. Next populate the addresses table. Note that if you have chosen a different schema name to 'staging' you will have to `sed` the .sql files as per step 4. Navigate to the `populate_tables` folder and run the following:
		
		psql -f POPULATE-staging-addresses.sql