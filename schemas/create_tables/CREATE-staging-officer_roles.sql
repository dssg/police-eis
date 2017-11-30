DROP TABLE IF EXISTS staging.officer_roles;
CREATE TABLE staging.officer_roles (
  officer_id                         INT REFERENCES staging.officers_hub (officer_id) ON DELETE CASCADE, --officer id
  department_defined_officer_id_code TEXT, --the department's internal officer identifier
  department_start_date              TIMESTAMP, -- date when the officer started in that department
  department_id                      VARCHAR(20), --department id number
  department_name                    TEXT, -- name of the department
  job_start_date                     TIMESTAMP, -- date the officer started the job
  job_id                             VARCHAR(20), --the title associated with the role e.g. patrol officer
  job_title                          TEXT, -- job name / description
  sworn_flag                         INT, -- flag for sworn officers 1 else 0
  rank_code                          INT, --rank associated with the role
  paygrade_code                      INT, --pay grade of the role
  police_area_id                     INT, --largest geographical grouping that the role is part of (e.g. Western District vs Eastern District)
  police_group_id                    INT, --largest task-type grouping the role is part of (e.g. Homicide, Organized Crime, Patrol, etc)
  duty_status_code                   INT                 --whether the role is active, inactive (e.g. parental leave), or terminated (e.g retired, resigned, or fired)
);
