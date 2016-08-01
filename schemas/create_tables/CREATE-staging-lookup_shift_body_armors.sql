DROP TABLE IF EXISTS staging.lookup_shift_body_armors; 
CREATE UNLOGGED TABLE staging.lookup_shift_body_armors (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
