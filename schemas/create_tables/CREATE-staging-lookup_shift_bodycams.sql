DROP TABLE IF EXISTS staging.lookup_shift_bodycams; 
CREATE UNLOGGED TABLE staging.lookup_shift_bodycams (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
