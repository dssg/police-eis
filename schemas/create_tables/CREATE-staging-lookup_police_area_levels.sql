DROP TABLE IF EXISTS staging.lookup_police_area_levels; 
CREATE  TABLE staging.lookup_police_area_levels (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar,            --
	parent_code                                                           int                 --
);
