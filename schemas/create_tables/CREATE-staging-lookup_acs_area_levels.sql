DROP TABLE IF EXISTS staging.lookup_acs_area_levels; 
CREATE UNLOGGED TABLE staging.lookup_acs_area_levels (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar,            --
	parent_code                                                           int                 --
);
