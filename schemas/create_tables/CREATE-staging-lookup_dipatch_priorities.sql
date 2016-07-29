DROP TABLE IF EXISTS staging.lookup_dipatch_priorities; 
CREATE UNLOGGED TABLE staging.lookup_dipatch_priorities (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
