/*
## Detailed SQL View Explanation

### `mvw_followups_sector_sla`

**Purpose:**  
This view calculates how long each sale (each `followup_id`) spent in each sector of the process.  
It's designed to help identify bottlenecks or delays in the sales pipeline and calculate SLAs.

**Business Logic:**
- Captures the time period each sale spent in a given sector (e.g., SALES, FORMALIZATION, etc.).
- Includes transitions from the initial undefined state (`UNDEFINED`) to `COMPLETED`.
- Uses log timestamps to determine when a sale entered and exited each sector.
- Results can be used to compute metrics like average, median, and percentiles of duration per sector.

**Use Case:**
This view allows the business team to identify stages in the sales flow where delays are most frequent and focus process improvements accordingly.
*/

CREATE MATERIALIZED VIEW public.mvw_followups_sector_sla AS 

WITH tb_data_criacao AS (
    -- Gets creation date for each sale (followup_id)
    SELECT
        dbdelivery.tb_mongo_parent_followups.id AS followup_id
        ,dbdelivery.tb_mongo_parent_followups.data_de_criacao AS followup_creation_date
        ,dbdelivery.tb_mongo_parent_followups.tipo AS operation
        ,dbdelivery.tb_mongo_parent_followups.plataforma_origem AS origin_platform
        ,dbdelivery.tb_mongo_users.convenio AS covenant
        ,dbdelivery.tb_mongo_parent_followups.comissao AS commission
    FROM dbdelivery.tb_mongo_parent_followups
    JOIN dbdelivery.tb_mongo_users 
      ON dbdelivery.tb_mongo_parent_followups.user_id = dbdelivery.tb_mongo_users.user_id 
),

tb_max_data_criacao AS (
    -- Captures the last log timestamp per sale
    SELECT
        followup_id
        ,followup_creation_date
        ,operation
        ,origin_platform
        ,covenant
        ,commission
        ,MAX(dbdelivery.tb_mongo_logs_followups.data_criacao) AS max_log_date
    FROM dbdelivery.tb_mongo_logs_followups
    JOIN tb_data_criacao USING (followup_id)
    GROUP BY 1,2,3,4,5,6
),

tb_mudancas_setor AS (
    -- Selects all sector changes for each sale
    SELECT
        dbdelivery.tb_mongo_logs_followups.followup_id
        ,followup_creation_date
        ,operation
        ,origin_platform
        ,covenant
        ,commission
        ,dbdelivery.tb_mongo_logs_followups.data_criacao AS log_date
        ,conteudo_original AS previous_sector
        ,conteudo_atualizado AS current_sector
    FROM dbdelivery.tb_mongo_logs_followups
    JOIN tb_data_criacao USING (followup_id)
    WHERE campo_alterado = 'Setor'
),

tb_min_data_mudanca_setor AS (
    -- Gets the first sector change log for each sale
    SELECT
        followup_id
        ,followup_creation_date
        ,operation
        ,origin_platform
        ,covenant
        ,commission
        ,MIN(log_date) AS first_log_date
    FROM tb_mudancas_setor
    GROUP BY 1,2,3,4,5,6
),

tb_data_completed AS (
    -- Gets the first timestamp when a sale reached the 'COMPLETED' sector
    SELECT
        followup_id
        ,followup_creation_date
        ,operation
        ,origin_platform
        ,covenant
        ,commission
        ,MIN(log_date) AS completed_date
    FROM tb_mudancas_setor
    WHERE current_sector = 'COMPLETED'
    GROUP BY 1,2,3,4,5,6
),

tb_periodos_setor AS (
    -- Creates start/end periods for each sector
    SELECT
        followup_id
        ,followup_creation_date
        ,operation
        ,origin_platform
        ,covenant
        ,commission
        ,log_date AS start_time
        ,current_sector AS sector
        ,LEAD(log_date) OVER (PARTITION BY followup_id ORDER BY log_date) AS end_time
    FROM tb_mudancas_setor

    UNION ALL

    -- Adds initial period starting from followup creation with sector 'UNDEFINED'
    SELECT
        dc.followup_id
        ,dc.followup_creation_date
        ,dc.operation
        ,dc.origin_platform
        ,dc.covenant
        ,dc.commission
        ,dc.followup_creation_date AS start_time
        ,'UNDEFINED' AS sector
        ,ms.first_log_date AS end_time
    FROM tb_data_criacao dc
    LEFT JOIN tb_min_data_mudanca_setor ms
      ON dc.followup_id = ms.followup_id
),

tb_duracao_setor AS (
    -- Calculates sector duration in seconds, limited by COMPLETED or max log date
    SELECT
        ps.followup_id
        ,ps.followup_creation_date
        ,ps.operation
        ,ps.origin_platform
        ,ps.covenant
        ,ps.commission
        ,ps.sector
        ,ps.start_time
        ,LEAST(
              COALESCE(ps.end_time, mdc.max_log_date),
              COALESCE(dc.completed_date, mdc.max_log_date)
          ) AS end_time
        ,DATEDIFF('second', ps.start_time, LEAST(
              COALESCE(ps.end_time, mdc.max_log_date),
              COALESCE(dc.completed_date, mdc.max_log_date)
          )) AS duration_seconds
    FROM tb_periodos_setor ps
    LEFT JOIN tb_max_data_criacao mdc ON ps.followup_id = mdc.followup_id
    LEFT JOIN tb_data_completed dc ON ps.followup_id = dc.followup_id
    WHERE ps.sector IS NOT NULL
      AND ps.start_time <= COALESCE(dc.completed_date, mdc.max_log_date)
)

SELECT
    followup_id
    ,followup_creation_date
    ,operation
    ,origin_platform
    ,covenant
    ,commission
    ,SUM(CASE WHEN sector = 'UNDEFINED'     THEN duration_seconds ELSE 0 END) AS time_undefined
    ,SUM(CASE WHEN sector = 'SALES'         THEN duration_seconds ELSE 0 END) AS time_sales
    ,SUM(CASE WHEN sector = 'TYPING'        THEN duration_seconds ELSE 0 END) AS time_typing
    ,SUM(CASE WHEN sector = 'FORMALIZATION' THEN duration_seconds ELSE 0 END) AS time_formalization
    ,SUM(CASE WHEN sector = 'PROGRESS'      THEN duration_seconds ELSE 0 END) AS time_progress
    ,SUM(CASE WHEN sector = 'COMPLETED'     THEN duration_seconds ELSE 0 END) AS time_completed
    ,SUM(CASE WHEN sector = 'CANCELED'      THEN duration_seconds ELSE 0 END) AS time_canceled
FROM tb_duracao_setor
GROUP BY 1,2,3,4,5,6;
