DROP TABLE IF EXISTS staging.lookup_stop_types; 
CREATE UNLOGGED TABLE staging.lookup_stop_types (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
