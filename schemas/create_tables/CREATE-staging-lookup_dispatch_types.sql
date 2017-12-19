DROP TABLE IF EXISTS staging.lookup_dispatch_types;
CREATE TABLE staging.lookup_dispatch_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
