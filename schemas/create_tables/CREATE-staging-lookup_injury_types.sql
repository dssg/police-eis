DROP TABLE IF EXISTS staging.lookup_injury_types; 
CREATE UNLOGGED TABLE staging.lookup_injury_types (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
