DROP TABLE IF EXISTS staging.addresses;
CREATE TABLE staging.addresses (
  address_id                      SERIAL PRIMARY KEY, --
  department_defined_reference_id VARCHAR UNIQUE,
  street_address                  TEXT, --street address
  second_street_address           TEXT, --unit/apartment
  city                            TEXT, --city
  zip                             INT, --zip code
  state                           TEXT, --state
  latitude                        NUMERIC, --
  longitude                       NUMERIC, --
  hundreds_block                  TEXT, --
  geom                            GEOMETRY(POINT, 4326),
  last_modified                   TIMESTAMP --timestamp for when the table was last updated

);
