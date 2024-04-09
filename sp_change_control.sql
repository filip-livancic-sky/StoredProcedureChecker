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
    effective_from_dt             TIMESTAMP     OPTIONS(description="Datetime of when the Stored Procedure was altered"),
    effective_to_dt               TIMESTAMP     OPTIONS(description="Datetime of when the Stored Procedure was overwritten"),
    status                        STRING        OPTIONS(description="Check of SP is Active or Deleted"),
);

/*Getting The latest list of procedures recently changed*/
CREATE OR REPLACE TEMP TABLE last_modified AS (
  SELECT
      TIMESTAMP_TRUNC(TIMESTAMP(CURRENT_DATETIME()), SECOND, 'UTC') AS exec_dt,
      CONCAT(routine_catalog,'.',routine_schema,'.',routine_name) AS stored_procedure_path,
      routine_catalog,
      routine_schema,
      routine_name,
      '"' || ddl || '"' AS ddl,
      TIMESTAMP_TRUNC(last_altered, SECOND, "UTC") AS effective_from_dt,
      TIMESTAMP('2999-12-31 23:59:59') AS effective_to_dt
  FROM `skyuk-uk-vis-cust-res-d1-lab.stored_procedures.INFORMATION_SCHEMA.ROUTINES`
);

/* Names of SP's Recently Added, Deleted, Updated or Unchanged */
CREATE OR REPLACE TEMP TABLE sp_change_state AS (

  WITH stage_1 AS (
  (SELECT stored_procedure_path,'New' AS change_state FROM last_modified
    EXCEPT DISTINCT
   SELECT stored_procedure_path, 'New' AS change_state FROM `skyuk-uk-vis-cust-res-d1-lab.tableau_broadband_team.sp_change_control`) 

    UNION ALL

  (SELECT stored_procedure_path,'Deleted' AS change_state FROM `skyuk-uk-vis-cust-res-d1-lab.tableau_broadband_team.sp_change_control` 
    EXCEPT DISTINCT
   SELECT stored_procedure_path,'Deleted' AS change_state FROM last_modified)
  ),

  stage_2 AS (
   (SELECT t.stored_procedure_path, 'Updated' AS change_state 
    FROM `skyuk-uk-vis-cust-res-d1-lab.tableau_broadband_team.sp_change_control` t
    JOIN last_modified lm
      ON t.stored_procedure_path = lm.stored_procedure_path
    WHERE t.effective_from_dt != lm.effective_from_dt
        EXCEPT DISTINCT
    SELECT s.stored_procedure_path, s.change_state FROM stage_1 s)

        UNION ALL

    SELECT s.stored_procedure_path, s.change_state FROM stage_1 s
  ),

  stage_3 AS (
    (SELECT t.stored_procedure_path, 'Unchanged' AS change_state 
    FROM `skyuk-uk-vis-cust-res-d1-lab.tableau_broadband_team.sp_change_control` t
    JOIN last_modified lm
      ON t.stored_procedure_path = lm.stored_procedure_path
    WHERE t.effective_from_dt = lm.effective_from_dt
        EXCEPT DISTINCT
    SELECT s.stored_procedure_path, s.change_state FROM stage_2 s)

        UNION ALL

    SELECT s.stored_procedure_path, s.change_state FROM stage_2 s
  )

  SELECT * FROM stage_3
);

/* Insert All Changes */
INSERT INTO `skyuk-uk-vis-cust-res-d1-lab.tableau_broadband_team.sp_change_control` (
  SELECT 
    lm.*,
    CASE
      WHEN cs.change_state = 'New' THEN 'Active'
      WHEN cs.change_state = 'Updated' THEN 'Active'
      WHEN cs.change_state = 'Deleted' THEN 'Deleted'
    END AS status
  FROM last_modified lm
  JOIN sp_change_state cs 
    ON cs.stored_procedure_path = lm.stored_procedure_path
  WHERE cs.change_state != 'Unchanged'
);

/* Find Deleted and Updated SP's */
CREATE OR REPLACE TEMP TABLE updated_or_deleted AS (
  SELECT 
      lm.exec_dt,
      lm.stored_procedure_path,
      lm.routine_catalog,
      lm.routine_schema,
      lm.routine_name,
      COALESCE(lm.ddl,t.ddl) AS ddl,
      lm.effective_from_dt,
      lm.effective_to_dt,
    CASE
      WHEN cs.change_state = 'Deleted' AND t.status = 'Active' THEN 'Deleted'
      WHEN cs.change_state = 'Updated' AND t.status = 'Active' THEN 'Overwritten' 
    END AS status
  FROM `skyuk-uk-vis-cust-res-d1-lab.tableau_broadband_team.sp_change_control` t
  JOIN sp_change_state cs ON t.stored_procedure_path = cs.stored_procedure_path
  JOIN last_modified lm   ON t.stored_procedure_path = lm.stored_procedure_path
  WHERE 
        t.status = 'Active'
    AND cs.change_state IN ('Deleted','Updated')
    AND t.effective_from_dt != lm.effective_from_dt
);

/* Updated older SP's Overwritten by Update or Delete Changes */
UPDATE `skyuk-uk-vis-cust-res-d1-lab.tableau_broadband_team.sp_change_control` t
SET 
    t.status = uod.status,
    t.effective_to_dt = uod.effective_from_dt
FROM updated_or_deleted uod
WHERE 
      t.stored_procedure_path = uod.stored_procedure_path
  AND t.exec_dt != uod.exec_dt
  AND t.status = 'Active'
;

END;
