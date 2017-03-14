DROP TABLE IF EXISTS staging.officer_roles; 
CREATE  TABLE staging.officer_roles (
	officer_id                                                            int references staging.officers_hub(officer_id) on delete cascade,                --officer id
        department_defined_officer_id_code                                    text,               --the department's internal officer identifier
        department_start_date                                                 timestamp,         -- date when the officer started in that department
        department_id                                                         varchar(20),       --department id number
        department_name                                                       TEXT,              -- name of the department
        job_start_date                                                        timestamp,         -- date the officer started the job
	job_id                                                                varchar(20),       --the title associated with the role e.g. patrol officer
        job_description                                                       TEXT,              -- job name / description
	rank_code                                                             int,                --rank associated with the role
	paygrade_code                                                         int,                --pay grade of the role
	police_area_id                                                        int,                --largest geographical grouping that the role is part of (e.g. Western District vs Eastern District)
	police_group_id                                                       int,                --largest task-type grouping the role is part of (e.g. Homicide, Organized Crime, Patrol, etc)
	duty_status_code                                                      int                 --whether the role is active, inactive (e.g. parental leave), or terminated (e.g retired, resigned, or fired)
);
