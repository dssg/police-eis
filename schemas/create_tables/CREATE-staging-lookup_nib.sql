DROP TABLE IF EXISTS staging.lookup_nib; 
CREATE UNLOGGED TABLE staging.lookup_nib (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
