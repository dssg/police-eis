DROP TABLE IF EXISTS staging.lookup_paygrades; 
CREATE UNLOGGED TABLE staging.lookup_paygrades (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
