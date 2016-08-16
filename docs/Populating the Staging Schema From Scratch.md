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
		
   If you have chosen a schema names other than 'staging' you will need to replace the schema name in the raw files... this can be done with a `sed` command (NB this modifies the files in-place so it's sensible to create a new directory first):
   
   		sed -i 's/staging\./<different_schema_name>\./g' *.sql 
	 	
5. The next stage is to run the pre-staging scripts, which add some stored procedures and create some indexed tables that make loading data into the staging schema considerably faster. Navigate to the private repo (`police-eis-private`), and run both:

		psql -f schemas/prestaging/cmpd/raw_table_modifications.sql
		psql -f schemas/stored_procedures/cmpd/lookup_codes_stored_procedures.sql
		
  If the schema is different from 'staging', run via:
  
  		sed 's/staging\./staging_dev\./g' lookup_codes_stored_procedures.sql | psql
		
 The first of these commands will take a long time to run (most likely several hours).

6. Populate the lookup tables. Navigate to `/schemas/poplate_tables/lookup/` in the public repo. Open `populate_lookup_tables.py` and change the `SCHEMA_NAME` variable to whatever you have chosen (if different from 'staging'). Then run:

		python populate_lookup_tables.py --credentials=default_profile.yaml --tablefile=lookup_tables.yaml

7. Next populate the addresses table. Note that if you have chosen a different schema name to 'staging' you will have to `sed` the .sql files as per step 5. Navigate to the `populate_tables` folder and run the following:
		
		psql -f POPULATE-staging-addresses.sql
		
8. Now the officers hub, and the spoke tables associated with it:

		psql -f POPULATE-staging-officers_hub.sql
		psql -f POPULATE-staging-officer_characteristics.sql
		psql -f POPULATE-staging-officer_marital.sql
		
9. Now ready to start with the events hub:

		psql -f POPULATE-staging-incidents-hub.sql
		psql -f POPULATE-staging-arrests-hub.sql
		psql -f POPULATE-staging-dispatches-hub.sql
		
10. Next add the indexes to the hub table:

		psql -f create_events_hub_indexes.sql
		
11. Now the spoke tables can be populated. Again, this may take quite some time:

		psql -f POPULATE-staging-incidents-spoke.sql
		psql -f POPULATE-staging-arrests-spoke.sql
		psql -f POPULATE-staging-dispatches-spoke.sql

12. Index the spoke tables:

		psql -f create_spoke_table_indexes.sql
		
13. And create the post-staging tables which have 1 row for each dispatch (hugely speeds up feature generation query). Again, this step may take quite some time. Four tables are created here, and indexes on them are created within the script itself:

		psql -f create_poststaging_tables_for_dispatches.sql

1. If for any reason a mistake is made and the events hub needs to be repopulated, first run the following, which empties the *just* events hub and drops all of the various indexes (i.e. you get back to the beginning of step 9):

		psql -f restart_events_hub.sql
		
1. Once the events hub has been populated, run the index creation script, which creates all the required indexes for "fast" (!) feature generation:


		