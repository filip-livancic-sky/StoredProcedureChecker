CREATE OR REPLACE PROCEDURE `skyuk-uk-vis-cust-res-d1-lab.stored_procedures.sp_change_control`()
OPTIONS (strict_mode=false)
BEGIN

CREATE TABLE IF NOT EXISTS `skyuk-uk-vis-cust-res-d1-lab.tableau_broadband_team.sp_change_control`  (
  /*column                        data_type     description */
    exec_dt                       TIMESTAMP     OPTIONS(description="Datetime of executing SP version check"),
    stored_procedure_path         STRING        OPTIONS(description="Full Path to Stored Procedure"),
    routine_catalog               STRING        OPTIONS(description="Project Name of Stored Procedure"),
    routine_schema                STRING        OPTIONS(description="Dataset Name of Stored Procedure"),
    routine_name                  STRING        OPTIONS(description="Routine Name of Stored Procedure"),
    ddl                           STRING        OPTIONS(description="Contains the Stored Procedure"),
    status                        STRING        OPTIONS(description="Check of SP is Active or Deleted"),
    effective_from_dt             TIMESTAMP     OPTIONS(description="Datetime of when the Stored Procedure was altered"),
    effective_to_dt               TIMESTAMP     OPTIONS(description="Datetime of when the Stored Procedure was overwritten")
);

CREATE OR REPLACE TEMP TABLE last_modified AS (
  SELECT
      TIMESTAMP_TRUNC(TIMESTAMP(CURRENT_DATETIME()), SECOND, 'UTC') AS exec_dt,
      CONCAT(routine_catalog,'.',routine_schema,'.',routine_name) AS stored_procedure_path,
      routine_catalog,
      routine_schema,
      routine_name,
      '"' || ddl || '"' AS ddl,
      'Active' AS status,
      TIMESTAMP_TRUNC(last_altered, SECOND, "UTC") AS effective_from_dt,
      TIMESTAMP('2999-12-31 23:59:59') AS effective_to_dt
  FROM `skyuk-uk-vis-cust-res-d1-lab.stored_procedures.INFORMATION_SCHEMA.ROUTINES`
);

/* Names of SP's Recently Added, but not in Main Table */
CREATE OR REPLACE TEMP TABLE new_sp_list AS (
  SELECT stored_procedure_path FROM last_modified
    EXCEPT DISTINCT
  SELECT stored_procedure_path FROM `skyuk-uk-vis-cust-res-d1-lab.tableau_broadband_team.sp_change_control` 
);

/* Insert NEW SP's */
INSERT INTO `skyuk-uk-vis-cust-res-d1-lab.tableau_broadband_team.sp_change_control` (
  SELECT lm.* FROM last_modified lm
  JOIN new_sp_list nsl 
    ON nsl.stored_procedure_path = lm.stored_procedure_path
);

/* Find SP's which have been updated */
CREATE OR REPLACE TEMP TABLE updated_sp AS (
  SELECT lm.*
  FROM `skyuk-uk-vis-cust-res-d1-lab.tableau_broadband_team.sp_change_control` scc
  JOIN last_modified lm
    ON scc.stored_procedure_path = lm.stored_procedure_path
  WHERE scc.effective_from_dt != lm.effective_from_dt
    AND scc.status = 'Active'
);

/* Update Table To Identify Entries That Are Overwritten */
UPDATE `skyuk-uk-vis-cust-res-d1-lab.tableau_broadband_team.sp_change_control` scc
SET status = 'Overwritten'
FROM updated_sp usp
WHERE 
      scc.stored_procedure_path = usp.stored_procedure_path
  AND scc.effective_to_dt = '2999-12-31 23:59:59';

/* Inserting UPDATED SP's */
INSERT INTO `skyuk-uk-vis-cust-res-d1-lab.tableau_broadband_team.sp_change_control` (
  SELECT * FROM updated_sp
);

/* Change Terminating Timestamps for the Overwritten Entries */
UPDATE `skyuk-uk-vis-cust-res-d1-lab.tableau_broadband_team.sp_change_control` scc
SET effective_to_dt = usp.effective_from_dt
FROM updated_sp usp
WHERE 
      scc.stored_procedure_path = usp.stored_procedure_path
  AND scc.effective_to_dt = '2999-12-31 23:59:59'
  AND scc.status = 'Overwritten'
;

/* Need to Add Section which Identifies Deleted Procedures */
/* Need to Add Section which Identifies Deleted Procedures */

END
