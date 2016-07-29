DROP TABLE IF EXISTS staging.lookup_genders; 
CREATE UNLOGGED TABLE staging.lookup_genders (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
