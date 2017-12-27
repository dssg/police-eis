DROP TABLE IF EXISTS staging.lookup_sky_conditions_total_coverages;
CREATE TABLE staging.lookup_sky_conditions_total_coverages (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
