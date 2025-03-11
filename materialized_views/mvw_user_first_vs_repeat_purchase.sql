/*
=====================================================================
  Project      : [konsiencia]
  Script       : mvw_user_first_vs_repeat_purchase.sql
  Author       : Thallys
  Created on   : 2025-03-11
  Last Update  : 2025-03-11
  Description  : Joins tb_mongo_parent_followups and tb_mongo_operators
---------------------------------------------------------------------
  Environment  : [Databricks | Redshift]
  Schema       : public
  Tables       : 
    - Input  : dbdelivery.tb_storm_contracts, tb_mongo_operators, tb_mongo_users
    - Output : public.mvw_user_first_vs_repeat_purchase
---------------------------------------------------------------------
  Dependencies : ETLs from dbdelivery.tb_storm_contracts, tb_mongo_operators, tb_mongo_users
  Notes        : Identifies each user's purchases (first-time purchase vs. repeat purchase)
=====================================================================
*/

WITH
-- ------------------------------------------------------------------------
-- 1) Join parent_followups with operators (to get seller name),
--    filtering only completed sales with valid user_id and commission.
-- ------------------------------------------------------------------------
pfo AS (
  SELECT 
     pf.* 
     ,op.nome AS seller_name
  FROM dbdelivery.tb_mongo_parent_followups pf
  LEFT JOIN dbdelivery.tb_mongo_operators op
         ON pf.vendedor_id = op.vendedor_id
  WHERE pf.status = 'Finalizado'
    AND pf.comissao >= 0
    AND pf.user_id IS NOT NULL
    AND pf.user_id != ''
),

-- ------------------------------------------------------------------------
-- 2) Join with tb_mongo_users and get CPF from tb_needs_info.
--    Adds ROW_NUMBER() to identify first-time vs. repeat purchases.
-- ------------------------------------------------------------------------
pfo_detailed AS (
  SELECT
    pfo.*
     ,ni.cpf
     ,ROW_NUMBER() OVER (
         PARTITION BY pfo.user_id
         ORDER BY COALESCE(pfo.data_de_finalizacao, pfo.data_de_criacao) ASC
       ) AS rn
  FROM pfo
  LEFT JOIN dbdelivery.tb_mongo_users ui 
         ON pfo.user_id = ui.user_id
  JOIN dbbusiness.tb_needs_info ni
         ON ui.user_id = ni.user_id -- Only available in schema dbbusiness to access CPF
)

-- ------------------------------------------------------------------------
-- 3) Final SELECT: one row = one purchase, with user/cpf and purchase type flag
-- ------------------------------------------------------------------------
SELECT
    COALESCE(
      DATE_TRUNC('month', pfo_detailed.data_de_finalizacao),
      DATE_TRUNC('month', pfo_detailed.data_de_criacao)
    ) AS month
   ,pfo_detailed.user_id                   AS user_id
   ,pfo_detailed.cpf                       AS cpf
   ,pfo_detailed.seller_name               AS seller
   ,pfo_detailed.plataforma_origem        AS platform
   ,CASE 
        WHEN pfo_detailed.rn = 1 THEN 'First-time purchase'
        ELSE 'Repeat purchase'
    END AS purchase_type
   ,pfo_detailed.comissao                 AS commission
   ,pfo_detailed.data_de_finalizacao      AS completion_date
   ,pfo_detailed.data_de_criacao          AS creation_date
FROM pfo_detailed;
