DROP TABLE IF EXISTS staging.lookup_persons; 
CREATE UNLOGGED TABLE staging.lookup_persons (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
