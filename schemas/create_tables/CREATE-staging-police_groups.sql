DROP TABLE IF EXISTS staging.police_groups; 
CREATE  TABLE staging.police_groups (
	police_group_id                                                       int,                --primary key for this area - unique for department_defined_id, area_level and valid_from_date combination
	police_group_department_defined_id                                    varchar,            --id for the area as defined by the particular department
	police_group_level_code                                               int,                --e.g. department > division > unit
	last_modified                                                         date,               --date on which the shapefile was recorded
	parent_police_group_id                                                int,                --group_id of the area one area_level up from this area
	police_group_name                                                     varchar,            --name given to this group by the police department
	police_group_description                                              text                --
);
