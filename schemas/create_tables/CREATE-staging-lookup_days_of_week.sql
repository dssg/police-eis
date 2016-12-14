DROP TABLE IF EXISTS staging.lookup_days_of_week;
CREATE UNLOGGED TABLE staging.lookup_days_of_week (
        code                                                                  int,                --
        value                                                                 varchar,            --
        description                                                           varchar             --
);
