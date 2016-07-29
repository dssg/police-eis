DROP TABLE IF EXISTS staging.lookup_hazards; 
CREATE UNLOGGED TABLE staging.lookup_hazards (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
