/*
## Detailed SQL View Explanation

### `mvw_user_funnel_daily_metrics`

**Goal:**  
This view consolidates key daily metrics from the user registration and sales funnel, including valid leads, blocked users, created follow-ups, and finalized sales, all grouped by day.

**Business Logic Highlights:**
- Identifies whether a user has a valid relationship (`Lead válido`) or is blocked.
- Tracks lead generation, proposal creation, and follow-up creation within 30 days after user registration.
- Removes follow-ups that went from “Incomplete” to “Canceled” to avoid inflating funnel numbers.

**Use Case:**  
Helps marketing and operations teams evaluate the effectiveness of the onboarding and conversion funnel on a daily basis.

*/

CREATE MATERIALIZED VIEW public.mvw_user_funnel_daily_metrics AS

WITH users AS (
    SELECT
        id
        ,creation_id
        ,user_id
        ,lead_id
        ,nome
        ,convenio AS covenant
        ,leadlinktype
        ,secretaria
        ,cpf
        ,aniversario
        ,numero_wpp
        ,email
        ,data_ultimo_acesso
        ,codigo_convenio
        ,data_criacao
        ,usuario_e_valido
        ,usuario_tem_proposta
        ,e_bloqueado 
        ,blacklisted
        ,ocupacao
        ,employer
        ,campaign_id
        ,origin_user
        ,registry
        ,plataforma_fonte
        ,ishardbounced
        ,e_lead_valido
        ,codigo_banco
        ,nome_banco
        ,numero_agencia
        ,numero_conta
    FROM dbdelivery.tb_mongo_users
),

-- Evaluate the eligibility of each user’s record (matrícula)
avaliacao_matriculas_validas AS (
    SELECT
          user_id 
        ,lead_id
        ,CASE
            WHEN (convenio != 'INSS' AND lead_link_type = 0) THEN 'valid'
            WHEN (convenio != 'INSS' AND lead_link_type != 0) THEN 'invalid'
            WHEN (convenio = 'INSS' 
                    AND registros_elegivel_emprestimo = true 
                    AND registros_liberado_emprestimo = true 
                    AND registros_possui_representante_legal = false 
                    AND registros_possui_pensao_alimenticia = false) THEN 'valid'
            WHEN (convenio = 'INSS' AND registros_liberado_emprestimo = false) THEN 'Blocked'
            ELSE 'invalid'
          END AS record_status
        ,registros_numero AS matricula
    FROM dbdelivery.tb_mongo_lead_users mlu
    WHERE 
        user_id IN (SELECT user_id FROM dbdelivery.tb_mongo_users)
        AND mlu.lead_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id, matricula ORDER BY data_criacao DESC) = 1
),

-- Evaluate users who have at least one valid record
leads_validos AS (
    SELECT 
          user_id
        ,CASE 
            WHEN SUM(CASE WHEN record_status = 'valid' THEN 1 ELSE 0 END) >= 1 THEN 'true'
            WHEN SUM(CASE WHEN record_status = 'Blocked' THEN -1 ELSE 0 END) < 0 THEN 'Blocked'
            ELSE 'false'
          END AS lead_valid
    FROM avaliacao_matriculas_validas
    GROUP BY user_id
),

-- Attach lead validity to user
mongo_users AS (
    SELECT
          u.*
        ,CASE
            WHEN u.e_bloqueado = true THEN 'Blocked'
            ELSE CASE 
                WHEN lv.lead_valid = 'true' THEN 'Valid lead'
                WHEN lv.lead_valid = 'false' THEN 'Ineligible'
                WHEN lv.lead_valid = 'Blocked' THEN 'Blocked'
                ELSE 'Not a lead'
              END
          END AS lead_status
    FROM users u
    LEFT JOIN leads_validos lv ON u.user_id = lv.user_id
),

-- Remove follow-ups where status changed from “Incomplete” to “Canceled”
logs_incompletos_para_cancelado AS (
    SELECT followup_id
    FROM dbdelivery.tb_mongo_logs_followups
    WHERE campo_alterado = 'Status' 
      AND conteudo_original IN ('Incompleto sem interação', 'Incompleto em negociação')
      AND conteudo_atualizado = 'Cancelado'
),

-- Core funnel logic
dados AS (
    SELECT DISTINCT
        DATE_TRUNC('day', u.data_criacao) AS day
        ,u.convenio AS covenant
        ,COALESCE(u.plataforma_fonte, 'APP') AS platform
        ,u.user_id
        ,l.lead_id
        ,e.user_id AS has_proposal
        ,f.user_id AS created_30d
        ,CASE
          WHEN f.status NOT IN ('Cancelado pelo usuário', 'Incompleto sem interação', 'Incompleto em negociação')
               AND f.id NOT IN (SELECT followup_id FROM logs_incompletos_para_cancelado)
          THEN f.user_id
        END AS valid_created_30d
        ,CASE
          WHEN f.status = 'Finalizado' THEN f.user_id
        END AS finalized_30d
    FROM dbdelivery.tb_mongo_users u
    LEFT JOIN dbdelivery.tb_mongo_lead_users l ON u.user_id = l.user_id AND l.lead_id IS NOT NULL
    LEFT JOIN dbdelivery.tb_konsidb_events e ON l.user_id = e.user_id 
         AND DATE_TRUNC('month', e.data_criacao) = DATE_TRUNC('month', u.data_criacao)
         AND e.evento_id = 'user_has_proposals'
    LEFT JOIN dbdelivery.tb_mongo_parent_followups f ON e.user_id = f.user_id
         AND DATE_TRUNC('day', f.data_de_criacao) BETWEEN DATE_TRUNC('day', u.data_criacao)
         AND DATEADD(day, 30, DATE_TRUNC('day', u.data_criacao))
),

-- Join with user status info
dados_com_status AS (
    SELECT
        d.*,
        m.lead_status
    FROM dados d
    LEFT JOIN mongo_users m ON d.user_id = m.user_id
)

SELECT
    day
    ,covenant
    ,platform
    ,CASE WHEN COUNT(DISTINCT user_id) > 0 THEN COUNT(DISTINCT user_id) ELSE NULL END AS total_registered
    ,CASE WHEN COUNT(DISTINCT lead_id) > 0 THEN COUNT(DISTINCT lead_id) ELSE NULL END AS lead_created
    ,CASE 
      WHEN COUNT(DISTINCT CASE WHEN lead_status IN ('Valid lead', 'Blocked') THEN user_id END) > 0 
      THEN COUNT(DISTINCT CASE WHEN lead_status IN ('Valid lead', 'Blocked') THEN user_id END)
      ELSE NULL 
    END AS valid_relationship
    ,CASE 
      WHEN COUNT(DISTINCT CASE WHEN lead_status = 'Valid lead' THEN user_id END) > 0 
      THEN COUNT(DISTINCT CASE WHEN lead_status = 'Valid lead' THEN user_id END)
      ELSE NULL 
    END AS unblocked_users
    ,CASE WHEN COUNT(DISTINCT has_proposal) > 0 THEN COUNT(DISTINCT has_proposal) ELSE NULL END AS proposal_opportunities
    ,CASE WHEN COUNT(DISTINCT created_30d) > 0 THEN COUNT(DISTINCT created_30d) ELSE NULL END AS followups_created_30d
    ,CASE WHEN COUNT(DISTINCT valid_created_30d) > 0 THEN COUNT(DISTINCT valid_created_30d) ELSE NULL END AS valid_followups_created_30d
    ,CASE WHEN COUNT(DISTINCT finalized_30d) > 0 THEN COUNT(DISTINCT finalized_30d) ELSE NULL END AS followups_finalized_30d
FROM dados_com_status
GROUP BY 1, 2, 3;
