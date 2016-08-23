DROP TABLE IF EXISTS staging.lookup_hair_colors; 
CREATE UNLOGGED TABLE staging.lookup_hair_colors (
	code                                                                  int,                --
	value                                                                 varchar,            --
	description                                                           varchar             --
);
