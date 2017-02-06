DROP TABLE IF EXISTS staging.officer_roles; 
CREATE  TABLE staging.officer_roles (
	officer_id                                                            int references staging.officers_hub(officer_id) on delete cascade,                --officer id
	last_modified                                                         timestamp,          --date and time the role was given
	rank_code                                                             int,                --rank associated with the role
	job_id                                                                int,                --the title associated with the role e.g. patrol officer
	paygrade_code                                                         int,                --pay grade of the role
	police_area_id                                                        int,                --largest geographical grouping that the role is part of (e.g. Western District vs Eastern District)
	police_group_id                                                       int,                --largest task-type grouping the role is part of (e.g. Homicide, Organized Crime, Patrol, etc)
	duty_status_code                                                      int                 --whether the role is active, inactive (e.g. parental leave), or terminated (e.g retired, resigned, or fired)
);
