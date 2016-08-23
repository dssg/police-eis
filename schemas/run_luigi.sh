#!/bin/bash

##########################################################
#USER: Change Schema to Desired Schema Name
##########################################################
export schema=staging_dev
echo $schema

##########################################################
#USER: No Need to Modify Below Code
##########################################################






##########################################################
#Ensure Setup
##########################################################

#Check for Config File
[ -f luigi.cfg ] && echo "Luigi Config File Exists" || echo "Create Luigi Config File with PSQL Credentials"

#Check to See if PG_Tools Cloned
[ -d pg_tools/.git ] && echo "PG_Tools Repository Exists" || { echo "Folder does not exist" && git clone --recursive git@github.com:jonkeane/pg_tools.git ; }


#Check to See if Police-EIS Private Cloned in Root
[ -d police-eis-private/.git ] && echo "Police-EIS-Private Repository Exists" || { echo "Folder does not exist" && git clone --recursive git@github.com:dssg/police-eis-private ; }

##########################################################
#Build Staging Tables from Create Schemas
##########################################################
PYTHONPATH='' luigi --module setupStaging CreateAllStagingTables --create-tables-directory ./create_tables/ --schema $schema --local-scheduler

##########################################################
#Build Lookup Tables
##########################################################
PYTHONPATH='' luigi --module setupStaging PopulateLookupTables --table-file ./populate_tables/lookup/lookup_tables.yaml --schema $schema --CreateAllStagingTables-create-tables-directory ./create_tables  --local-scheduler

#Copy Credentials to Private EIS Repository
cp luigi.cfg police-eis-private/schemas/

#Change to Private EIS Repository
cd police-eis-private/schemas/
pwd

##########################################################
#Populate SQL Stored Procedures
##########################################################
PYTHONPATH='' luigi --module populateStagingFromMNPD PopulateStoredProcedures --schema $schema --local-scheduler

##########################################################
#Populate Staging Schemas from ETL Schemas
##########################################################
PYTHONPATH='' luigi --module populateStagingFromMNPD PopulateAllStagingTables --schema $schema --populate-tables-directory ./populate_tables/mnpd --local-scheduler
