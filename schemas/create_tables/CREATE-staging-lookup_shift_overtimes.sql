DROP TABLE IF EXISTS staging.lookup_shift_overtimes;
CREATE TABLE staging.lookup_shift_overtimes (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
