DROP TABLE IF EXISTS staging.officer_characteristics; 
CREATE UNLOGGED TABLE staging.officer_characteristics (
	officer_id                                                            int references staging.officers_hub(officer_id) on delete cascade,                --officer id
	weight_pounds                                                         float,              --weight in pounds
	height_inches                                                         float,              --height in inches
	gender_code                                                           int,                --gender
	military_service_flag                                                 bool,               --whether the officer has served in the military
	education_level_code                                                  int,                --the level of education recieved e.g. college degree
	field_of_study_code                                                   int,                --what subject they primarily studied
	create_time                                                           timestamp,          --datetime when data was created
	last_modified                                                         timestamp,          --datetime when data was last modified/updated
	officer_first_name                                                    varchar,            --
	officer_middle_name                                                   varchar,            --
	officer_last_name                                                     varchar,            --
	officer_suffix                                                        varchar,            --
	officer_prefix                                                        varchar             --
);
