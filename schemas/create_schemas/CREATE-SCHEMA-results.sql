DROP SCHEMA IF EXISTS results CASCADE;
CREATE SCHEMA results;

-- model table containing each of the models run.
CREATE TABLE results.models(
	model_id                    serial primary key,
    run_time                    timestamp,
    batch_run_time              timestamp,
	config 				        json,
    pickle_file                 bytea 
);

-- predictions corresponding to each model.
CREATE TABLE results.predictions(
		model_id                    								int references results.models(model_id),
    unit_id                     								bigint,
    unit_score                  								numeric,
    label_value                 								int
);

-- evaluation table containing metrics for each of the models run.
CREATE TABLE results.evaluations(
		model_id                   									int references results.models(model_id),
		accuracy_score															numeric,
		auc_score			             									numeric,
		roc_auc_score																numeric,
		average_precision_score											numeric,
		f1_score																		numeric,
		fbeta_score_favor_precision									numeric,
		fbeta_score_favor_recall										numeric,
    precision_score_default					    				numeric,
    precision_score_at_top_point_01_percent     numeric,
    precision_score_at_top_point_1_percent      numeric,
		precision_score_at_top_1_percent          	numeric,
    precision_score_at_top_5_percent          	numeric,
    precision_score_at_top_10_percent          	numeric,
    recall_score_default					  	  				numeric,
		recall_score_at_top_point_01_percent				numeric,
		recall_score_at_top_point_1_percent					numeric,
		recall_score_at_top_1_percent								numeric,
		recall_score_at_top_5_percent								numeric,
		recall_score_at_top_10_percent							numeric,
		time_for_model_in_seconds										numeric
);
