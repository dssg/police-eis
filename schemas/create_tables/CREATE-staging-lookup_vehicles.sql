DROP TABLE IF EXISTS staging.lookup_vehicles;
CREATE TABLE staging.lookup_vehicles (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
