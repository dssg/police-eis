DROP TABLE IF EXISTS staging.lookup_education_levels; 
CREATE UNLOGGED TABLE staging.lookup_education_levels (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
