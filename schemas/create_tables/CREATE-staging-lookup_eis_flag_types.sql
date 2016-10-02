DROP TABLE IF EXISTS staging.lookup_eis_flag_types; 
CREATE UNLOGGED TABLE staging.lookup_eis_flag_types (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
