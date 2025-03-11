/*
## Detailed SQL View Explanation

### `mvw_monthly_growth_metrics`

**Goal:**  
This view aggregates multiple business metrics per month, allowing the company to monitor growth in terms of user acquisition, client base, qualified leads, and revenue (commission). 
It helps analyze the impact of marketing and sales efforts month over month.

**Business Logic Highlights:**
- Tracks first-time appearances of covenants, users, and clients.
- Calculates cumulative sums for leads, users, and clients to monitor growth.
- Counts unique clients per month and their respective total commissions.
- Isolates commissions generated specifically by first-time buyers.

**Use Case:**  
Used by business and marketing teams to evaluate user base growth, funnel efficiency, and the economic impact of new clients. Commonly visualized in dashboards for strategic planning.

*/

CREATE MATERIALIZED VIEW public.mvw_monthly_growth_metrics AS 

WITH first_covenant_appearance AS (
    SELECT
        convenio AS covenant
        ,MIN(DATE_TRUNC('month', data_criacao)) AS first_appearance_month
    FROM dbdelivery.tb_mongo_users
    WHERE convenio IS NOT NULL AND convenio != '' AND convenio NOT ILIKE 'desconhecido'
    GROUP BY convenio
), 

new_covenants AS (
    SELECT
        first_appearance_month AS month
        ,COUNT(covenant) AS new_covenants
    FROM first_covenant_appearance
    GROUP BY first_appearance_month
),

cumulative_new_covenants AS (
    SELECT
        month
        ,new_covenants
        ,SUM(new_covenants) OVER (ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum
    FROM new_covenants
),

leads_users AS (
    SELECT DISTINCT
        dbdelivery.tb_mongo_users.user_id
        ,l.lead_id,
        ,dbdelivery.tb_mongo_users.data_criacao
        ,CASE 
            WHEN dbdelivery.tb_mongo_users.e_bloqueado IS NULL THEN 'false'
            ELSE dbdelivery.tb_mongo_users.e_bloqueado
        END AS is_blocked
    FROM dbdelivery.tb_mongo_users
    JOIN dbdelivery.tb_mongo_lead_users l ON dbdelivery.tb_mongo_users.user_id = l.user_id
),

qualified_leads_per_month AS (
    SELECT 
        COUNT(*) AS users
        ,DATE_TRUNC('month', data_criacao) AS month
        CASE 
            WHEN (lead_id IS NULL OR is_blocked IS NULL) THEN 'Not qualified'
            ELSE 'Qualified'
        ,END AS classification
    FROM leads_users
    WHERE classification = 'Qualified'
    GROUP BY month, classification
),

cumulative_qualified_leads AS (
    SELECT
        month
        ,SUM(users) OVER (ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum
    FROM qualified_leads_per_month
),

first_client_payment AS (
    SELECT
        cpf_cliente
        ,MIN(DATE_TRUNC('month', data_pagamento)) AS first_appearance_month
    FROM dbdelivery.tb_storm_contracts
    WHERE cpf_cliente IS NOT NULL AND cpf_cliente != '' AND data_pagamento IS NOT NULL AND comissao_total >= 0
    GROUP BY cpf_cliente
),

first_client_commission AS (
    SELECT
        SUM(comissao_total) AS first_purchase_commission
        ,DATE_TRUNC('month', data_pagamento) AS month
    FROM dbdelivery.tb_storm_contracts
    JOIN first_client_payment ON 
        dbdelivery.tb_storm_contracts.cpf_cliente = first_client_payment.cpf_cliente
        AND DATE_TRUNC('month', dbdelivery.tb_storm_contracts.data_pagamento) = first_client_payment.first_appearance_month
    WHERE comissao_total >= 0
    GROUP BY 2
),

monthly_unique_clients AS (
    SELECT
        COUNT(DISTINCT cpf_cliente) AS unique_clients
        ,SUM(comissao_total) AS total_commission
        ,DATE_TRUNC('month', data_pagamento) AS month
    FROM dbdelivery.tb_storm_contracts
    WHERE cpf_cliente IS NOT NULL AND cpf_cliente != '' AND data_pagamento IS NOT NULL AND comissao_total >= 0
    GROUP BY month
),

new_clients_per_month AS (
    SELECT
        first_appearance_month AS month
        ,COUNT(cpf_cliente) AS new_clients
    FROM first_client_payment
    GROUP BY first_appearance_month
),

cumulative_new_clients AS (
    SELECT
        month
        ,new_clients
        ,SUM(new_clients) OVER (ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum
    FROM new_clients_per_month
),

first_user_appearance AS (
    SELECT
        user_id
        ,MIN(DATE_TRUNC('month', data_criacao)) AS first_appearance_month
    FROM dbdelivery.tb_mongo_users
    WHERE user_id IS NOT NULL AND user_id != ''
    GROUP BY user_id
),

new_users_per_month AS (
    SELECT
        first_appearance_month AS month
        ,COUNT(user_id) AS new_users
    FROM first_user_appearance
    GROUP BY first_appearance_month
),

cumulative_new_users AS (
    SELECT
        month
        ,new_users
        ,SUM(new_users) OVER (ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum
    FROM new_users_per_month
)

SELECT
    cumulative_qualified_leads.month,
    ,cumulative_new_covenants.cumulative_sum AS "Total Covenants"
    ,cumulative_new_users.cumulative_sum AS "User Base"
    ,cumulative_qualified_leads.cumulative_sum AS "Qualified Users"
    ,cumulative_new_clients.cumulative_sum AS "Total Clients"
    ,monthly_unique_clients.unique_clients AS "Clients This Month"
    ,monthly_unique_clients.total_commission AS "Commission This Month"
    ,cumulative_new_clients.new_clients AS "New Clients"
    ,first_client_commission.first_purchase_commission AS "First Purchase Commission"
FROM cumulative_new_users
LEFT JOIN cumulative_new_covenants USING(month)
LEFT JOIN cumulative_qualified_leads USING(month)
LEFT JOIN cumulative_new_clients USING(month)
LEFT JOIN monthly_unique_clients USING(month)
LEFT JOIN first_client_commission USING(month)
WHERE cumulative_qualified_leads.month >= '2022-01-01';
