DROP TABLE IF EXISTS staging.lookup_ncic; 
CREATE UNLOGGED TABLE staging.lookup_ncic (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
