DROP TABLE IF EXISTS staging.lookup_agencies; 
CREATE UNLOGGED TABLE staging.lookup_agencies (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
