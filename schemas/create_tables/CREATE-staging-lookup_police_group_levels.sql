DROP TABLE IF EXISTS staging.lookup_police_group_levels; 
CREATE  TABLE staging.lookup_police_group_levels (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar,            --
	parent_code                                                           int                 --
);
