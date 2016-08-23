DROP TABLE IF EXISTS staging.lookup_weapon_types; 
CREATE UNLOGGED TABLE staging.lookup_weapon_types (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
