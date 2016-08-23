DROP TABLE IF EXISTS staging.lookup_outcomes; 
CREATE UNLOGGED TABLE staging.lookup_outcomes (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
