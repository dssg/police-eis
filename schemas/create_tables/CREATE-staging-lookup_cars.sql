DROP TABLE IF EXISTS staging.lookup_cars;
CREATE TABLE staging.lookup_cars (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
