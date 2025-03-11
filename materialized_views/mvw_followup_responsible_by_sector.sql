/* 
## Detailed SQL View Explanation

### `mvw_followup_responsible_by_sector`

**Purpose:**  
Identifies the responsible person for each sector change in the followup (sales process).

**Business Logic:**  
- Every time a `followup_id` changes sector, a log is created.
- This view joins log and user tables to track who triggered the change.
- It also calculates whether the change is part of a successful funnel flow (e.g., from SALES to TYPING, FORMALIZATION, etc.).

**Use Case:**  
This view helped the BI team track accountability across teams and identify bottlenecks in handoffs between sales stages.
*/ 

CREATE MATERIALIZED VIEW public.mvw_followup_responsible_by_sector AS 
WITH 
ordered AS (
  SELECT
        followup_id
       ,dbdelivery.tb_mongo_users.user_id
       ,dbdelivery.tb_mongo_users.convenio AS covenant
       ,tipo AS operation  
       ,banco_destino AS destination_bank
       ,plataforma_origem AS origin_platform 
       ,dbdelivery.tb_mongo_logs_followups.data_criacao AS log_date 
       ,dbdelivery.tb_mongo_parent_followups.data_de_criacao AS followup_creation_date
       ,campo_alterado AS changed_field
       ,conteudo_original AS original_content
       ,conteudo_atualizado AS updated_content
       ,LAG(campo_alterado) OVER (PARTITION BY followup_id ORDER BY dbdelivery.tb_mongo_logs_followups.data_criacao) AS previous_field
       ,CASE 
           WHEN LAG(conteudo_atualizado) OVER (PARTITION BY followup_id ORDER BY dbdelivery.tb_mongo_logs_followups.data_criacao) 
                IN ('UNDEFINED', 'CANCELED', 'SALES', 'TYPING', 'FORMALIZATION', 'PROGRESS', 'COMPLETED') 
           THEN '' 
           ELSE LAG(conteudo_atualizado) OVER (PARTITION BY followup_id ORDER BY dbdelivery.tb_mongo_logs_followups.data_criacao) 
         END AS previous_content
       ,razao_cancelamento AS cancellation_reason
  FROM 
       dbdelivery.tb_mongo_logs_followups
       JOIN dbdelivery.tb_mongo_parent_followups  
         ON dbdelivery.tb_mongo_logs_followups.followup_id = dbdelivery.tb_mongo_parent_followups.id
       JOIN dbdelivery.tb_mongo_users
         ON dbdelivery.tb_mongo_parent_followups.user_id = dbdelivery.tb_mongo_users.user_id
  WHERE TRUE 
        AND dbdelivery.tb_mongo_parent_followups.data_de_criacao >= '2024-08-01' -- Implementation date of the 'Stage Responsible' field
        AND campo_alterado IN ('Sector', 'Stage Responsible')
)

SELECT 
    followup_id
     ,user_id
     ,covenant
     ,operation
     ,destination_bank
     ,origin_platform
     ,followup_creation_date
     ,log_date
     ,original_content 
     ,updated_content 
     ,previous_content AS stage_responsible
     ,CASE 
           WHEN original_content = 'UNDEFINED' AND updated_content = 'SALES' THEN 1
           WHEN original_content = 'SALES' AND updated_content = 'TYPING' THEN 1
           WHEN original_content = 'TYPING' AND updated_content = 'FORMALIZATION' THEN 1
           WHEN original_content = 'FORMALIZATION' AND updated_content = 'PROGRESS' THEN 1
           WHEN original_content = 'PROGRESS' AND updated_content = 'COMPLETED' THEN 1
           ELSE 0
       END AS is_conversion -- We consider conversion only for followups that completed the correct flow
     ,cancellation_reason
FROM 
     ordered
WHERE TRUE
      AND changed_field = 'Sector' 
      AND stage_responsible <> '';
