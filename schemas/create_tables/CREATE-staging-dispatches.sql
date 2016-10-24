DROP TABLE IF EXISTS staging.dispatches ;
CREATE UNLOGGED TABLE staging.dispatches (
  event_id                        INT REFERENCES staging.events_hub (event_id) PRIMARY KEY, --Primary key
  dispatch_id                     BIGINT, --
  dispatch_address_id             INT, --
  police_area_id                  INT, --
  unit_division                   VARCHAR,
  unit_beat                       VARCHAR,
  unit_type                       VARCHAR,
  unit_shift                      VARCHAR,
  unit_call_sign                  VARCHAR,
  dispatch_type_code              INT, --the reason for the dispatch
  dispatch_original_type          VARCHAR,
  dispatch_original_subtype       VARCHAR,
  dispatch_final_type             VARCHAR,
  dispatch_final_subtype          VARCHAR,
  response_plan_code              INT, --result of the dispatch
  dispatch_original_priority_code INT, --
  dispatch_final_priority_code    INT, --
  travel_time_minutes             INT, --
  response_time_minutes           INT, --
  time_on_scene_minutes           INT, --
  datetime_assigned               TIMESTAMP, --
  datetime_arrived                TIMESTAMP, --
  datetime_cleared                TIMESTAMP, --
  sequence_assigned               INT,
  sequence_arrived                INT,
  units_assigned                  INT,
  units_arrived                   INT,
  dispatch_category               VARCHAR,
  event_datetime                  TIMESTAMP
);

CREATE OR REPLACE FUNCTION create_partition_and_insert_dispatches()
  RETURNS TRIGGER AS
$BODY$
DECLARE
  partition_date_month TEXT;
  partition_name       TEXT;
  start_date           TEXT;
  end_date             TEXT;
  index_name           TEXT;
  stmt                 TEXT;
  partition_check      TEXT;
BEGIN
  partition_date_month := 'y' || extract(YEAR FROM NEW.event_datetime) || 'm' ||
                          lpad(extract(MONTH FROM NEW.event_datetime) :: TEXT, 2, '0');
  partition_name := TG_TABLE_SCHEMA || '.' || TG_RELNAME || '_' || partition_date_month;



  --IF to_regclass(partition_name::cstring) NOTNULL
  partition_check := TG_RELNAME || '_' || partition_date_month;
  IF NOT EXISTS(SELECT relname
                FROM pg_class
                WHERE relname = partition_check)
  THEN

    -- get start and end dates for the check constraint
    start_date := cast(date_trunc('MONTH', NEW.event_datetime) AS DATE);
    end_date := (cast(date_trunc('MONTH', NEW.event_datetime) AS DATE) + INTERVAL '1 MONTH') :: DATE;

    -- prepare the index name for the date on the partition column
    index_name := TG_RELNAME || '_' || partition_date_month || '_date';

    -- Execute the create table and the create index
    EXECUTE 'CREATE UNLOGGED TABLE ' || partition_name|| ' (
      CHECK ( event_datetime >=' || quote_literal(start_date) || ' AND event_datetime <' || quote_literal(end_date) || ')
    )
      INHERITS (' || TG_TABLE_SCHEMA || '.' || TG_RELNAME || ')';

    EXECUTE 'CREATE INDEX ' || index_name || '
      ON ' || partition_name || '(event_datetime)';


    RAISE NOTICE 'A partition has been created %', partition_name;
  END IF;

  EXECUTE 'INSERT INTO ' || partition_name || ' VALUES ($1.*)' USING NEW;
  RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;


DROP TRIGGER IF EXISTS insert_disptach_trigger
ON staging.dispatches;


CREATE TRIGGER insert_disptach_trigger
BEFORE INSERT ON staging.dispatches
FOR EACH ROW EXECUTE PROCEDURE create_partition_and_insert_dispatches();
