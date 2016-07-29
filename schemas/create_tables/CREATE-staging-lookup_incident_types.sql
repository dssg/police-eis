DROP TABLE IF EXISTS staging.lookup_incident_types; 
CREATE UNLOGGED TABLE staging.lookup_incident_types (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
