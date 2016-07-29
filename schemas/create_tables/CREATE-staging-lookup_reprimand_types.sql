DROP TABLE IF EXISTS staging.lookup_reprimand_types; 
CREATE UNLOGGED TABLE staging.lookup_reprimand_types (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
