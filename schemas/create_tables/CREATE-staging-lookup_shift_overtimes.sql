DROP TABLE IF EXISTS staging.lookup_shift_overtimes; 
CREATE UNLOGGED TABLE staging.lookup_shift_overtimes (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
