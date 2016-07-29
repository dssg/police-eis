DROP TABLE IF EXISTS staging.lookup_landmark_types; 
CREATE UNLOGGED TABLE staging.lookup_landmark_types (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
