DROP TABLE IF EXISTS staging.lookup_arrest_status; 
CREATE UNLOGGED TABLE staging.lookup_arrest_status (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
