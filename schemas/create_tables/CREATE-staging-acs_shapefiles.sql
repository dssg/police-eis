DROP TABLE IF EXISTS staging.acs_shapefiles; 
CREATE TABLE staging.acs_shapefiles (
	acs_area_id                                                           int,                --Census (ACS) Block Group Id
	acs_area_level_code                                                   varchar,            --ACS Block Group
	acs_area_valid_from_date                                              int,                --Year of ACS Data
	parent_acs_area_id                                                    varchar,            --Census (ACS) Id for chosen ACS area level, such as census tract id
	acs_area_name                                                         varchar,            --?
	acs_area_description                                                  varchar,            --?
	acs_area_geometry                                                     float,              --?
	acs_projection_type                                                   varchar             --?
);
