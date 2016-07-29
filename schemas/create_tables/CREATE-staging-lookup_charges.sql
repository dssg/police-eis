DROP TABLE IF EXISTS staging.lookup_charges; 
CREATE UNLOGGED TABLE staging.lookup_charges (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
