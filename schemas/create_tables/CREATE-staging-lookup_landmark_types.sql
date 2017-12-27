DROP TABLE IF EXISTS staging.lookup_landmark_types;
CREATE TABLE staging.lookup_landmark_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
