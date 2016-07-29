DROP TABLE IF EXISTS staging.lookup_ranks; 
CREATE UNLOGGED TABLE staging.lookup_ranks (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
