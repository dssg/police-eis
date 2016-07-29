DROP TABLE IF EXISTS staging.lookup_dispatch_types; 
CREATE UNLOGGED TABLE staging.lookup_dispatch_types (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
