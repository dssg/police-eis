DROP TABLE IF EXISTS staging.lookup_weapon_types;
CREATE TABLE staging.lookup_weapon_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
