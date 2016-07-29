DROP SCHEMA IF EXISTS models CASCADE;
CREATE SCHEMA models;

CREATE TABLE models.full(
	id_timestamp 		timestamp,
	config 				json,
	auc 				numeric
);
