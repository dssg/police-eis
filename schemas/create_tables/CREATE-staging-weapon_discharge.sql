DROP TABLE IF EXISTS staging.weapon_discharge;
CREATE TABLE staging.weapon_discharge (
    event_id                                     INT REFERENCES staging.events_hub (event_id),
    department_defined_weapon_discharge_id      VARCHAR, -- key that the dept uses internally

    unintentional_discharge                      BOOL,
    target_code                                  INT,  -- links to lookup_discharge_targets
    officer_weapon_type_code                     INT,  -- links to lookup_weapon_type
    protective_vest_flag                         BOOL, -- if the officer wore a protective vest
    lighting_condition_code                      INT,  -- links to lookup_lighting_condition
    officer_shots_fired                          INT,  -- number of shots the officer fired
    shots_fired_flag                             BOOL, -- if the actor (not the officer) fired shots
    -- on_duty_flag will be in the events hub

    -- the following flags could also be coded as a lookup code (and they are, in other parts 
    -- of the pipeline), but the nature of a weapon discharge case can be mixed (e.g., drug-related
    -- as well as burglary-related)
    incident_burglary_flag                       BOOL, -- if the officer responded to a burglary
    incident_robbery_flag                        BOOL, -- if the officer reponded to a robbery
    incident_drug_related_flag                   BOOL, -- if the officer reponded in a drug-related incident
    incident_traffic_stop_flag                   BOOL, -- if the officer discharged their weapon during a traffic stop
    incident_prisoner_flag                       BOOL, -- if the officer was with a prisoner
    incident_disturbance_flag                    BOOL, -- if the officer was responding to a disturbance
    incident_domestic_flag                       BOOL, -- if the officer was responding to a domestic case
    incident_bar_fight_flag                      BOOL, -- if the officer was responding to a bar fight
    incident_armed_person_flag                   BOOL, -- if the officer was responding to 'man with a gun'
    incident_animal_flag                         BOOL, -- if the officer was responding to an animal-related case (e.g. injured deer)
    incident_narrative                           VARCHAR, -- free text that describes the incident

    officer_injured_code                         INT, -- injury level of the officer, links to lookup_injuries
    officer_injury_self_flag                     BOOL, -- if the injury was self-inflicted
    -- In the following, we do not consider animals targets - if the officer shot an injured deer,
    -- the below code should be null.
    target_injured_code                          INT, -- injury level of the (non-animal) target or bystander; links to lookup_injuries

    asst_chief_approval_flag                     BOOL, -- a PBP-specific flag of unknown meaning at the moment
    chief_approval_flag                          BOOL, -- a PBP-specific flag of unknown meaning at the moment

    report_datetime                              TIMESTAMP,
    event_datetime                               TIMESTAMP,
    officer_id                                   INT REFERENCES staging.officers_hub (officer_id)
)
