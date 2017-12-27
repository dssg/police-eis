DROP TABLE IF EXISTS staging.patrol_divisions;
CREATE  TABLE staging.patrol_divisions (
    division_id               VARCHAR(5),
    division                  TEXT,
    response_area             int,
    geom                      GEOMETRY(MultiPolygon,4326)
);
