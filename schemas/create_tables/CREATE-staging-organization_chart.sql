DROP TABLE IF EXISTS staging.organization_chart; 
CREATE  TABLE staging.organization_chart (
	job_id                                                                int,                --unique primary key identifying a job e.g. patrol officer
	department_code                                                       int,                --foreign key in lookup_department_codes
	supervisor_id                                                         int,                --job_id of the supervisor  e.g. patrol officer supervisor
	supervisor_flag                                                       bool,               --is this job a supervisory position?
	title                                                                 varchar,            --
	description                                                           varchar             --
);
