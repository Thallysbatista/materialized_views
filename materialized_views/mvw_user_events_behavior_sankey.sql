-- This script analyzes user behavior by tracking their interaction with app screens and events,
-- ultimately producing a Sankey diagram to visualize their flow.

WITH events_data AS (
  SELECT
    ke.user_id,
    ke.data_criacao AS event_timestamp,
    mu.data_criacao AS user_creation_date,
    evento_id,
    plataforma_fonte AS user_origin,
    plataforma,
    mu.convenio AS covenant
  FROM dbdelivery.tb_konsidb_events ke
  JOIN dbdelivery.tb_mongo_users mu
    ON ke.user_id = mu.user_id
  WHERE mu.convenio IN ('PREFEITURA DE GOIÂNIA', 'GOVERNO DO RIO DE JANEIRO', 'GOVERNO DE AMAZONAS', 'GOVERNO DO PARANÁ')
    AND evento_id IN (
      'registration_completed', 'registration_completed_sms', 'registration_completed_email',
      'opened_easy_consig_registries_screen', 'opened_registries_screen', 'opened_govpe_registries_screen',
      'clicked_more_registry_easy_consig_screen', 'clicked_easy_consig_registries_support',
      'clicked_registries_screen_support_button', 'clicked_del_registry_easy_consig_screen',
      'clicked_finish_btn_easy_consig_screen', 'gave_registry',
      'view_lead_async_load_without_password', 'view_bank_wheel_screen_without_password',
      'clicked_load_async_support_without_psw', 'lead_generation_success', 'lead_generation_success_api',
      'lead_generation_failed', 'clicked_blocked_portability_button', 'clicked_blocked_max_change_button',
      'clicked_blocked_fit_change_button', 'clicked_blocked_instlment_red_btn', 'clicked_new_credit',
      'opened_new_credit_oportunities_view', 'clicked_open_password_recovery', 'gave_password',
      'gave_password_on_dashboard', 'clicked_dont_know_consigfacil_section', 'clicked_dont_know_app_section',
      'credentials_screen_opened', 'opened_dashboard_psw_screen', 'clicked_credentials_support_with_psw',
      'view_lead_async_load_with_password', 'view_bank_wheel_screen_with_password',
      'clicked_load_async_support_with_psw', 'lead_gen_with_psw_success', 'lead_gen_with_psw_failed',
      'clicked_credentials_error_screen_support', 'opened_wrong_psw_screen', 'open_credentials_error_screen',
      'clicked_back_to_opportunities', 'clicked_try_password_again', 'clicked_recovery_password_with_psw',
      'clicked_try_again_blocked_screen', 'open_blocked_access_screen', 'clicked_blocked_access_screen_support',
      'clicked_back_to_opportunities', 'entered_new_credit_no_opportunity', 'gave_psw_during_contracting',
      'opened_contracting_psw_screen', 'clicked_operation_done_continue_button', 'opened_operation_done_screen',
      'clicked_operation_done_support_button', 'no_lead_user_is_unlinked',
      'clicked_instabillity_error_screen_try_again_button', 'open_instability_error_screen',
      'clicked_instabillity_error_screen_support_button', 'no_lead_user_incorrect_covenant',
      'no_lead_covenant_confirmation_screen', 'no_lead_user_correct_covenant', 'wrong_registry_error_screen_opened',
      'clicked_wrong_registry_screen_retry_btn', 'clicked_wrong_registry_support_btn',
      'clicked_easy_consig_registries_support', 'clicked_blocked_benefit_card', 'splash_screen_view',
      'opened_unavailable_access_screen', 'clicked_support_unavailable_access'
    )
),

registration_completed AS (
  -- This CTE identifies when users completed the registration process
  SELECT 
    user_id,
    user_creation_date,
    user_origin,
    plataforma,
    covenant,
    MIN(event_timestamp) AS event_timestamp
  FROM events_data
  WHERE evento_id IN ('registration_completed', 'registration_completed_email', 'registration_completed_sms')
  GROUP BY 1, 2, 3, 4, 5
),

viewed_bank_wheel_no_password AS (
  -- Selecting only the first time a user accessed the bank wheel screen without password
  -- This ensures we capture the user's first action in this step of the funnel
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id IN ('view_lead_async_load_without_password', 'view_bank_wheel_screen_without_password', 'splash_screen_view')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

support_on_bank_wheel_screen AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id = 'clicked_load_async_support_without_psw'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

support_on_bank_wheel_screen_with_password AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id = 'clicked_load_async_support_with_psw'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

lead_success_no_password AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id IN ('lead_generation_success', 'lead_generation_success_api')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

lead_failure_no_password AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id = 'lead_generation_failed'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

unavailable_access_screen AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id = 'opened_unavailable_access_screen'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

registries_screen_after_unavailable_access AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id IN ('opened_easy_consig_registries_screen', 'opened_registries_screen')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

gave_password_after_unavailable_access AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id = 'gave_password'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

support_on_unavailable_access_screen AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id = 'clicked_support_unavailable_access'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

support_on_registry_screen_after_unavailable_access AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id IN ('clicked_registries_screen_support_button', 'clicked_easy_consig_registries_support')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

support_on_password_screen_after_unavailable_access AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id = 'clicked_credentials_support_with_psw'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

no_linked_registry AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id = 'no_lead_user_is_unlinked'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

instability_screen AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id = 'open_instability_error_screen'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

no_lead_confirmation_screen AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id = 'no_lead_covenant_confirmation_screen'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

wrong_registry_or_password_screen AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id IN ('opened_wrong_psw_screen', 'open_credentials_error_screen')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

blocked_user_screen AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id = 'open_blocked_access_screen'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

clicked_blocked_buttons AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id IN (
    'clicked_blocked_portability_button', 'clicked_blocked_max_change_button',
    'clicked_blocked_fit_change_button', 'clicked_blocked_instlment_red_btn', 'clicked_blocked_benefit_card')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

credentials_screen_after_blocked_buttons AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id IN ('credentials_screen_opened', 'opened_dashboard_psw_screen')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

gave_password_on_credentials_screen AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id IN ('gave_password', 'gave_password_on_dashboard')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

clicked_reset_password_on_credentials_screen AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id IN ('clicked_open_password_recovery', 'clicked_recovery_password_with_psw')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

clicked_support_on_credentials_screen AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id = 'clicked_credentials_support_with_psw'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

clicked_new_credit AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id = 'clicked_new_credit'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

no_new_credit_opportunity AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id = 'entered_new_credit_no_opportunity'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

opened_new_credit_opportunities_screen AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id = 'opened_new_credit_oportunities_view'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

opened_contracting_credentials_screen AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id IN ('opened_contracting_psw_screen', 'credentials_screen_opened')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

provided_password_on_contracting_screen AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id IN ('gave_password', 'gave_psw_during_contracting')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

viewed_bank_wheel_with_password AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id IN ('view_lead_async_load_with_password', 'view_bank_wheel_screen_with_password', 'splash_screen_view')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

lead_success_with_password AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id IN ('lead_gen_with_psw_success', 'lead_generation_success_api')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

lead_failure_with_password AS (
  SELECT 
    user_id,
    event_timestamp,
    user_creation_date,
    user_origin,
    plataforma,
    covenant
  FROM events_data
  WHERE evento_id = 'lead_gen_with_psw_failed'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) = 1
),

-- Step 1 of the funnel: Users who completed registration and then viewed the bank wheel without password
registration_completed_then_viewed_bank_wheel_no_password AS (
  SELECT 
    rc.user_id,
    vw.event_timestamp,
    rc.user_creation_date,
    rc.user_origin,
    rc.plataforma,
    rc.covenant
  FROM registration_completed rc
  JOIN viewed_bank_wheel_no_password vw
    ON rc.user_id = vw.user_id 
    AND rc.event_timestamp < vw.event_timestamp -- Ensure the user passed through the previous funnel step
),

-- Step 2 of the funnel: From bank wheel (no password) to support click
viewed_bank_wheel_to_support_click AS (
  SELECT 
    step1.user_id,
    sr.event_timestamp,
    step1.user_creation_date,
    step1.user_origin,
    step1.plataforma,
    step1.covenant
  FROM registration_completed_then_viewed_bank_wheel_no_password step1
  JOIN clicked_support_without_password sr
    ON step1.user_id = sr.user_id
    AND step1.event_timestamp < sr.event_timestamp
),

-- Step 2 of the funnel: From bank wheel (no password) to successful lead (no password)
viewed_bank_wheel_to_lead_success_no_password AS (
  SELECT 
    step1.user_id,
    ls.event_timestamp,
    step1.user_creation_date,
    step1.user_origin,
    step1.plataforma,
    step1.covenant
  FROM registration_completed_then_viewed_bank_wheel_no_password step1
  JOIN lead_success_without_password ls
    ON step1.user_id = ls.user_id
    AND step1.event_timestamp < ls.event_timestamp
),

-- Step 2 of the funnel: From bank wheel (no password) to lead failure (no password)
viewed_bank_wheel_to_lead_failure_no_password AS (
  SELECT 
    step1.user_id,
    lf.event_timestamp,
    step1.user_creation_date,
    step1.user_origin,
    step1.plataforma,
    step1.covenant
  FROM registration_completed_then_viewed_bank_wheel_no_password step1
  JOIN lead_failure_without_password lf
    ON step1.user_id = lf.user_id
    AND step1.event_timestamp < lf.event_timestamp
),

-- Step 3 of the funnel: From successful lead (no password) to user unlinked screen
lead_success_no_password_to_unlinked_screen AS (
  SELECT 
    prev.user_id,
    sv.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM viewed_bank_wheel_to_lead_success_no_password prev
  JOIN unlinked_user sv
    ON prev.user_id = sv.user_id
    AND prev.event_timestamp < sv.event_timestamp
),

-- From successful lead (no password) to blocked button (portability)
lead_success_no_password_to_blocked_portability AS (
  SELECT 
    prev.user_id,
    bp.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM viewed_bank_wheel_to_lead_success_no_password prev
  JOIN blocked_portability_button bp
    ON prev.user_id = bp.user_id
    AND prev.event_timestamp < bp.event_timestamp
),

-- From successful lead (no password) to clicked new credit
lead_success_no_password_to_clicked_new_credit AS (
  SELECT 
    prev.user_id,
    nc.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM viewed_bank_wheel_to_lead_success_no_password prev
  JOIN clicked_new_credit nc
    ON prev.user_id = nc.user_id
    AND prev.event_timestamp < nc.event_timestamp
),

-- From failed lead (no password) to blocked screen
lead_failure_no_password_to_blocked_screen AS (
  SELECT 
    prev.user_id,
    bs.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM viewed_bank_wheel_to_lead_failure_no_password prev
  JOIN blocked_screen bs
    ON prev.user_id = bs.user_id
    AND prev.event_timestamp < bs.event_timestamp
),

-- From failed lead (no password) to no lead confirmation screen
lead_failure_no_password_to_no_lead_screen AS (
  SELECT 
    prev.user_id,
    nl.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM viewed_bank_wheel_to_lead_failure_no_password prev
  JOIN no_lead_confirmation nl
    ON prev.user_id = nl.user_id
    AND prev.event_timestamp < nl.event_timestamp
),

-- From failed lead (no password) to instability error screen
lead_failure_no_password_to_instability_screen AS (
  SELECT 
    prev.user_id,
    ins.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM viewed_bank_wheel_to_lead_failure_no_password prev
  JOIN instability_screen ins
    ON prev.user_id = ins.user_id
    AND prev.event_timestamp < ins.event_timestamp
),

-- From failed lead (no password) to unavailable access screen
lead_failure_no_password_to_unavailable_access AS (
  SELECT 
    prev.user_id,
    ua.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM viewed_bank_wheel_to_lead_failure_no_password prev
  JOIN unavailable_access ua
    ON prev.user_id = ua.user_id
    AND prev.event_timestamp < ua.event_timestamp
),

-- Step 4 of the funnel: Lead success without password → blocked port → credential screen
lead_success_no_password_blocked_port_to_credential_screen AS (
  SELECT 
    prev.user_id,
    ts.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM lead_success_no_password_to_blocked_portability prev
  JOIN credential_screen_after_blocked_port ts
    ON prev.user_id = ts.user_id 
    AND prev.event_timestamp < ts.event_timestamp
),

-- Blocked port → clicked new → saw new credit opportunities screen
lead_success_no_password_blocked_port_to_new_credit_opportunities AS (
  SELECT 
    prev.user_id,
    nv.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM lead_success_no_password_to_blocked_portability prev
  JOIN viewed_new_credit_opportunities nv
    ON prev.user_id = nv.user_id 
    AND prev.event_timestamp < nv.event_timestamp
),

-- Blocked port → clicked new → no new opportunities screen
lead_success_no_password_blocked_port_to_no_new_credit_opportunity AS (
  SELECT 
    prev.user_id,
    sn.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM lead_success_no_password_to_blocked_portability prev
  JOIN no_new_credit_opportunity sn
    ON prev.user_id = sn.user_id 
    AND prev.event_timestamp < sn.event_timestamp
),

-- Lead failure → unavailable access → registration screen
lead_failure_unavailable_access_to_registration_screen AS (
  SELECT 
    prev.user_id,
    tm.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM lead_failure_no_password_to_unavailable_access prev
  JOIN registration_screen_after_unavailable_access tm
    ON prev.user_id = tm.user_id 
    AND prev.event_timestamp < tm.event_timestamp
),

-- Lead failure → unavailable access → clicked support
lead_failure_unavailable_access_to_support_click AS (
  SELECT 
    prev.user_id,
    sc.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM lead_failure_no_password_to_unavailable_access prev
  JOIN support_click_after_unavailable_access sc
    ON prev.user_id = sc.user_id 
    AND prev.event_timestamp < sc.event_timestamp
),

-- Step 5 of the funnel: Credential screen → gave password
credential_screen_after_blocked_port_to_password_given AS (
  SELECT 
    prev.user_id,
    gp.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM lead_success_no_password_blocked_port_to_credential_screen prev
  JOIN password_given_after_credential gp
    ON prev.user_id = gp.user_id 
    AND prev.event_timestamp < gp.event_timestamp
),

-- Credential screen → clicked reset password
credential_screen_after_blocked_port_to_reset_password AS (
  SELECT 
    prev.user_id,
    rp.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM lead_success_no_password_blocked_port_to_credential_screen prev
  JOIN reset_password_after_credential rp
    ON prev.user_id = rp.user_id 
    AND prev.event_timestamp < rp.event_timestamp
),

-- Credential screen → clicked support
credential_screen_after_blocked_port_to_support_click AS (
  SELECT 
    prev.user_id,
    sp.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM lead_success_no_password_blocked_port_to_credential_screen prev
  JOIN support_click_after_credential sp
    ON prev.user_id = sp.user_id 
    AND prev.event_timestamp < sp.event_timestamp
),

-- New credit opportunities screen → credential screen after blocked port
lead_success_no_password_blocked_port_to_new_credit_opportunities_to_credential_screen AS (
  SELECT 
    prev.user_id,
    cs.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM lead_success_no_password_blocked_port_to_new_credit_opportunities prev
  JOIN viewed_credential_screen_after_new_credit cs
    ON prev.user_id = cs.user_id 
    AND prev.event_timestamp < cs.event_timestamp
),

-- Step 6 of the funnel: Credential screen after blocked port → gave password → viewed bank wheel with password
credential_screen_after_blocked_port_to_password_given_to_bank_wheel AS (
  SELECT 
    prev.user_id,
    rb.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM credential_screen_after_blocked_port_to_password_given prev
  JOIN bank_wheel_with_password rb
    ON prev.user_id = rb.user_id 
    AND prev.event_timestamp < rb.event_timestamp
),

-- Credential screen → reset password (after viewing opportunities)
viewed_opportunities_to_credential_screen_to_reset_password AS (
  SELECT 
    prev.user_id,
    rs.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM lead_success_no_password_blocked_port_to_new_credit_opportunities_to_credential_screen prev
  JOIN reset_password_after_credential rs
    ON prev.user_id = rs.user_id 
    AND prev.event_timestamp < rs.event_timestamp
),

-- Credential screen → clicked support (after viewing opportunities)
viewed_opportunities_to_credential_screen_to_support_click AS (
  SELECT 
    prev.user_id,
    sp.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM lead_success_no_password_blocked_port_to_new_credit_opportunities_to_credential_screen prev
  JOIN support_click_after_credential sp
    ON prev.user_id = sp.user_id 
    AND prev.event_timestamp < sp.event_timestamp
),

-- Credential screen → gave password (after viewing opportunities)
viewed_opportunities_to_credential_screen_to_password_given AS (
  SELECT 
    prev.user_id,
    gp.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM lead_success_no_password_blocked_port_to_new_credit_opportunities_to_credential_screen prev
  JOIN password_given_after_credential gp
    ON prev.user_id = gp.user_id 
    AND prev.event_timestamp < gp.event_timestamp
),
-- Step 7 of the funnel: Password given → bank wheel with password → clicked support
password_given_to_bank_wheel_to_support_click AS (
  SELECT 
    pg.user_id,
    sp.event_timestamp,
    pg.user_creation_date,
    pg.user_origin,
    pg.plataforma,
    pg.covenant
  FROM credential_screen_after_blocked_port_to_password_given_to_bank_wheel pg
  JOIN support_click_after_bank_wheel sp
    ON pg.user_id = sp.user_id 
    AND pg.event_timestamp < sp.event_timestamp
),

-- Password given → bank wheel with password → lead generation success
password_given_to_bank_wheel_to_lead_success AS (
  SELECT 
    pg.user_id,
    ls.event_timestamp,
    pg.user_creation_date,
    pg.user_origin,
    pg.plataforma,
    pg.covenant
  FROM credential_screen_after_blocked_port_to_password_given_to_bank_wheel pg
  JOIN lead_success_with_password ls
    ON pg.user_id = ls.user_id 
    AND pg.event_timestamp < ls.event_timestamp
),

-- Password given → bank wheel with password → lead generation failed
password_given_to_bank_wheel_to_lead_failure AS (
  SELECT 
    pg.user_id,
    lf.event_timestamp,
    pg.user_creation_date,
    pg.user_origin,
    pg.plataforma,
    pg.covenant
  FROM credential_screen_after_blocked_port_to_password_given_to_bank_wheel pg
  JOIN lead_failure_with_password lf
    ON pg.user_id = lf.user_id 
    AND pg.event_timestamp < lf.event_timestamp
),

-- Path after new credit opportunity credential screen: password given → bank wheel
opportunity_to_credential_to_password_to_bank_wheel AS (
  SELECT 
    prev.user_id,
    rb.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM viewed_opportunities_to_credential_screen_to_password_given prev
  JOIN bank_wheel_with_password rb
    ON prev.user_id = rb.user_id 
    AND prev.event_timestamp < rb.event_timestamp
),

-- Step 8 of the funnel: Second bank wheel → lead generation success
second_bank_wheel_to_lead_success AS (
  SELECT 
    prev.user_id,
    ls.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM opportunity_to_credential_to_password_to_bank_wheel prev
  JOIN lead_success_with_password ls
    ON prev.user_id = ls.user_id 
    AND prev.event_timestamp < ls.event_timestamp
),

-- Second bank wheel → lead generation failure
second_bank_wheel_to_lead_failure AS (
  SELECT 
    prev.user_id,
    lf.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM opportunity_to_credential_to_password_to_bank_wheel prev
  JOIN lead_failure_with_password lf
    ON prev.user_id = lf.user_id 
    AND prev.event_timestamp < lf.event_timestamp
),

-- Step 9 of the funnel: second bank wheel → support
second_bank_wheel_to_support AS (
  SELECT 
    prev.user_id,
    sp.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM opportunity_to_credential_to_password_to_bank_wheel prev
  JOIN support_click_after_bank_wheel sp
    ON prev.user_id = sp.user_id 
    AND prev.event_timestamp < sp.event_timestamp
),

-- First bank wheel → lead success → no link
bank_wheel_to_lead_success_to_no_link AS (
  SELECT 
    prev.user_id,
    nl.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM password_given_to_bank_wheel_to_lead_success prev
  JOIN no_link_screen nl
    ON prev.user_id = nl.user_id
    AND prev.event_timestamp < nl.event_timestamp
),

-- First bank wheel → lead failed → user block
bank_wheel_to_lead_failure_to_user_block AS (
  SELECT 
    prev.user_id,
    ub.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM password_given_to_bank_wheel_to_lead_failure prev
  JOIN user_block_screen ub
    ON prev.user_id = ub.user_id
    AND prev.event_timestamp < ub.event_timestamp
),

-- First bank wheel → lead failed → wrong password/matriculation
bank_wheel_to_lead_failure_to_wrong_password AS (
  SELECT 
    prev.user_id,
    wp.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM password_given_to_bank_wheel_to_lead_failure prev
  JOIN wrong_password_screen wp
    ON prev.user_id = wp.user_id
    AND prev.event_timestamp < wp.event_timestamp
),

-- Second bank wheel → lead success → no link
second_bank_wheel_to_lead_success_to_no_link AS (
  SELECT 
    prev.user_id,
    nl.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM second_bank_wheel_to_lead_success prev
  JOIN no_link_screen nl
    ON prev.user_id = nl.user_id
    AND prev.event_timestamp < nl.event_timestamp
),

-- Second bank wheel → lead failure → user block
second_bank_wheel_to_lead_failure_to_user_block AS (
  SELECT 
    prev.user_id,
    ub.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM second_bank_wheel_to_lead_failure prev
  JOIN user_block_screen ub
    ON prev.user_id = ub.user_id
    AND prev.event_timestamp < ub.event_timestamp
),

-- Second bank wheel → lead failure → wrong password/matriculation
second_bank_wheel_to_lead_failure_to_wrong_password AS (
  SELECT 
    prev.user_id,
    wp.event_timestamp,
    prev.user_creation_date,
    prev.user_origin,
    prev.plataforma,
    prev.covenant
  FROM second_bank_wheel_to_lead_failure prev
  JOIN wrong_password_screen wp
    ON prev.user_id = wp.user_id
    AND prev.event_timestamp < wp.event_timestamp
),

final AS (
  SELECT * FROM (
    SELECT 1 AS step_order, 'Registration Completed' AS source, 'Bank Wheel' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM registration_completed_viewed_bank_wheel
    UNION ALL
    SELECT 2 AS step_order, 'Bank Wheel' AS source, 'Support - Bank Wheel' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM bank_wheel_clicked_support
    UNION ALL
    SELECT 3 AS step_order, 'Bank Wheel' AS source, 'Lead Without Password' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM bank_wheel_lead_success_no_password
    UNION ALL
    SELECT 4 AS step_order, 'Lead Without Password' AS source, 'Unlinked User' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM lead_success_no_password_unlinked
    UNION ALL
    SELECT 5 AS step_order, 'Lead Without Password' AS source, 'Clicked New Option' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM lead_success_no_password_clicked_new
    UNION ALL
    SELECT 6 AS step_order, 'Clicked New Option' AS source, 'Viewed New Opportunities Screen' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM clicked_new_viewed_opportunities
    UNION ALL
    SELECT 7 AS step_order, 'Viewed New Opportunities Screen' AS source, 'Credential Screen 2' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM opportunities_screen_to_credential_screen
    UNION ALL
    SELECT 8 AS step_order, 'Credential Screen 2' AS source, 'Reset Password 2' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM credential_screen_2_reset_password
    UNION ALL
    SELECT 9 AS step_order, 'Credential Screen 2' AS source, 'Support 2' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM credential_screen_2_support
    UNION ALL
    SELECT 10 AS step_order, 'Credential Screen 2' AS source, 'Entered Password 2' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM credential_screen_2_entered_password
    UNION ALL
    SELECT 11 AS step_order, 'Entered Password 2' AS source, 'Bank Wheel With Password 2' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM entered_password_to_bank_wheel_2
    UNION ALL
    SELECT 12 AS step_order, 'Bank Wheel With Password 2' AS source, 'Support - Bank Wheel 3' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM bank_wheel_2_support
    UNION ALL
    SELECT 13 AS step_order, 'Bank Wheel With Password 2' AS source, 'Lead With Password 2' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM bank_wheel_2_lead_success
    UNION ALL
    SELECT 14 AS step_order, 'Lead With Password 2' AS source, 'Unlinked User 3' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM lead_success_with_password_unlinked_3
    UNION ALL
    SELECT 15 AS step_order, 'Bank Wheel With Password 2' AS source, 'Lead With Password Failed 2' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM bank_wheel_2_lead_failed
    UNION ALL
    SELECT 16 AS step_order, 'Lead With Password Failed 2' AS source, 'User Blocked 3' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM lead_failed_user_blocked_3
    UNION ALL
    SELECT 17 AS step_order, 'Lead With Password Failed 2' AS source, 'Wrong Credentials 2' AS target, user_id, user_creation_date, event_timestamp, user_origin, plataforma, covenant FROM lead_failed_wrong_credentials_2
  )
)

SELECT 
  step_order,
  source,
  target,
  COUNT(*) AS users
FROM final
GROUP BY 1, 2, 3



