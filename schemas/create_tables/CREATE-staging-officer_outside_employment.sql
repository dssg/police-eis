DROP TABLE IF EXISTS staging.officer_outside_employment;
CREATE TABLE staging.officer_outside_employment (
  officer_id        INT REFERENCES staging.officers_hub (officer_id) ON DELETE CASCADE, --officer id
  external_job_code INT, --job that the officer had outside of the police force. e.g. security guard
  date_time         TIMESTAMP, --datetime that the job started
  hours_on_shift    FLOAT, --number of hours the officer worked
  hourly_rate       FLOAT, -- how much is paid by hour
  in_uniform_flag   BOOL, --whether the officer was in police uniform when at this job i.e. were they in the role of a police officer
  address_raw       VARCHAR, --address of where the job was
  address_id        INT                 --link to addresses table
);
