DROP TABLE IF EXISTS staging.lookup_shift_bodycams;
CREATE TABLE staging.lookup_shift_bodycams (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
