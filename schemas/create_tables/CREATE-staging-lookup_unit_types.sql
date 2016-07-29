DROP TABLE IF EXISTS staging.lookup_unit_types; 
CREATE UNLOGGED TABLE staging.lookup_unit_types (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
