DROP TABLE IF EXISTS staging.police_area_shapefiles; 
CREATE  TABLE staging.police_area_shapefiles (
	police_area_id                                                        serial,                --primary key for this area - unique for department_defined_id, area_level and valid_from_date combination
	police_area_department_defined_id                                     varchar,            --id for the area as defined by the particular department
	police_area_level_code                                                int,                --1 for coarsest shapefile level ... x for most granular level: PK formed by area_id and area_level together
	last_modified                                                         date,               --date on which the shapefile was recorded (created)
	parent_police_area_id                                                 int,                --area_id of the area one area_level up from this area
	police_area_name                                                      varchar,            --name given to this shapefile by the police department e.g. north, downtown
	police_area_description                                               varchar,            --description given to this shapefile by the PD
	police_area_geometry                                                  geometry,              --postgis encoding of the polygon shapefile
	police_projection_type                                                varchar             --string of the projection type - need to know this to be able to convert to lat/long co-ords 
);
