DROP TABLE IF EXISTS staging.dispatches CASCADE;
CREATE UNLOGGED TABLE staging.dispatches (
  event_id                        INT REFERENCES staging.events_hub (event_id), --Primary key (cannot be passed on in partitions, would require advisory lock when inserting)
  dispatch_id                     BIGINT, --
  dispatch_address_id             INT REFERENCES staging.addresses (address_id), --
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
  event_datetime                  TIMESTAMP,
  officer_id                      INT REFERENCES staging.officers_hub (officer_id)

);
