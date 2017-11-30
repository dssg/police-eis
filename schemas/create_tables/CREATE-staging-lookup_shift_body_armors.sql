DROP TABLE IF EXISTS staging.lookup_shift_body_armors;
CREATE TABLE staging.lookup_shift_body_armors (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
