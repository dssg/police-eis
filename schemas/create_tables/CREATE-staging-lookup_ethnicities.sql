DROP TABLE IF EXISTS staging.lookup_ethnicities; 
CREATE UNLOGGED TABLE staging.lookup_ethnicities (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
