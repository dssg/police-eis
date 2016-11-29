DROP SCHEMA IF EXISTS feedback CASCADE;
CREATE SCHEMA feedback;

-- model table containing each of the models run.
CREATE TABLE feedback.officer_feedback (
    model_id                                                                                            int references results.models(model_id), --should be a foreign key to results.models table
    reviewer_id                                                                                         int,
    review_time                                                                                         timestamp,
    department_defined_officer_id                                                                       text,
    risk_score_agree                                                                                    bool,
    risk_1_agree                                                                                        bool,
    risk_2_agree                                                                                        bool,
    risk_3_agree                                                                                        bool,
    risk_4_agree                                                                                        bool,
    risk_5_agree                                                                                        bool,
    comments                                                                                            text
);
