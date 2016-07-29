DROP TABLE IF EXISTS staging.lookup_arrest_types; 
CREATE UNLOGGED TABLE staging.lookup_arrest_types (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
