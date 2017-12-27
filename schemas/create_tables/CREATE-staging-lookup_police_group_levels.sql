DROP TABLE IF EXISTS staging.lookup_police_group_levels;
CREATE TABLE staging.lookup_police_group_levels (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR, --
  parent_code INT                 --
);
