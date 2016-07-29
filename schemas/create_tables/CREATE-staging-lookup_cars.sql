DROP TABLE IF EXISTS staging.lookup_cars; 
CREATE UNLOGGED TABLE staging.lookup_cars (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
