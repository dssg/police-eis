DROP TABLE IF EXISTS staging.lookup_ucr4_grouped_dispatch_types;
CREATE TABLE staging.lookup_ucr4_grouped_dispatch_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
