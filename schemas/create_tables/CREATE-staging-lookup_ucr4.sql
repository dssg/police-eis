DROP TABLE IF EXISTS staging.lookup_ucr4; 
CREATE UNLOGGED TABLE staging.lookup_ucr4 (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
