CREATE TABLE @pathway_analysis_codes
(
	pathway_analysis_generation_id BIGINT NOT NULL,
	code BIGINT NOT NULL,
	name VARCHAR(2000) NOT NULL,
	is_combo int NOT NULL
);
