DROP TABLE IF EXISTS staging.events_hub CASCADE;
CREATE TABLE staging.events_hub (
  event_id                    SERIAL PRIMARY KEY, -- unique id for the event
  event_chain_id              INT, -- id for the set of events that are linked together
  source_event_id             INT, -- unique id in the source table of this event.
  department_defined_event_id VARCHAR, --
  department_source_id_hashed UUID, -- note: MD5 hashes can be stored as UUID
  officer_id                  INT REFERENCES staging.officers_hub (officer_id), --
  event_type_code             INT, --
  event_datetime              TIMESTAMP, --
  shift_id                    INT, --
  dispatch_id                 BIGINT,
  off_duty_flag               BOOL, --
  last_moddate                TIMESTAMP DEFAULT now() -- last time this row was modified
);
