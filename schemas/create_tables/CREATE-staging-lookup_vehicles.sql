DROP TABLE IF EXISTS staging.lookup_vehicles; 
CREATE UNLOGGED TABLE staging.lookup_vehicles (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
