WITH dat AS (
  SELECT distinct 
  mp.project_name ,
    e.id as e_id ,
    ed.id as ed_id ,
      d.name AS `File Name`,
      mp.updated_date AS `Completed On`,
      d.patient_identifier AS `Member ID`,
      e.patient_last_name AS `Member Last Name`,
      e.patient_first_name AS `Member First Name`,
      e.patient_middle_name AS `Member Middle Name`,
      TIMESTAMPDIFF(YEAR, e.patient_birth_date, ed.encounter_actual_period_start) AS Age,
      e.patient_gender AS Gender,
      ed.visit_type_id AS `Visit Type`,
      ed.encounter_actual_period_start AS `From DOS`,
      ed.encounter_actual_period_end   AS `To DOS`,
      ed.encounter_practitioner_identifier  AS `Rendering Provider NPI ID`,
      ed.encounter_practitioner_first_name  AS `Rendering Provider Name First`,
      ed.encounter_practitioner_last_name   AS `Rendering Provider Name Last`,
      ed.encounter_address_hospital_name    AS `Rendering Provider Address Line 1`,
      ed.encounter_address_text             AS `Rendering Provider Address Line 2`,
      ed.encounter_address_city             AS `Rendering Provider City`,
      ed.encounter_address_postal_code      AS `Rendering Provider Postal Code`,
      ed.encounter_address_state            AS `Rendering Provider Address State Code`,

    --   GROUP_CONCAT(dm.subject , ',') as `Comment Field`,
    CASE 
        WHEN ed.id is null then GROUP_CONCAT(dc.comments , ',')
     else GROUP_CONCAT(DISTINCT dos_de.evidence_text SEPARATOR ', ') end as 'Comment Field' ,

      GROUP_CONCAT(de.evidence_text , ',') as `Evidence Comment`,


      c.condition_code AS `Diag`,
      v24.hcc_group_name        AS v24_code,
      ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY v24.hcc_group_name)         AS v24_row,
      v28.cms_hcc_v28_group_name    AS v28_code,
      ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY v28.cms_hcc_v28_group_name  )     AS v28_row,
      rx.rxhcc_hcc_group_name   AS rxhcc_code,
      ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY rx.rxhcc_hcc_group_name)    AS rxhcc_row
  FROM encounter_mst e
  JOIN (
      SELECT encounter_id, process_id, updated_date,user_specific_comments , 
          standard_comments, project_name
      FROM (
        SELECT
          esmv.encounter_id,
          esmv.process_id,
          esmv.updated_date,
          p.user_specific_comments,
          p.standard_comments,
          p.name as project_name,
          ROW_NUMBER() OVER (
            PARTITION BY esmv.encounter_id
            ORDER BY esmv.process_id DESC
          ) AS rn
        FROM ra_audit_apigateway.encounter_status_map_view esmv
        JOIN ra_audit_apigateway.project_mst p
          ON p.id = esmv.project_id and p.is_active = 1
        WHERE p.name = 'CHPW_2024_Calibration_Batch'
          AND esmv.encounter_status_id IN (8, 9)
          AND esmv.client_name = 'CHPW'
          and esmv.is_active = 1
      ) x 
      where rn = 1  
  ) mp
    ON e.id = mp.encounter_id 
  JOIN document_mst d
    ON d.encounter_id = e.id and d.is_active =1 
    left join discussion_mst dm 
    on dm.encounter_id = e.id and dm.is_active = 1 
  left join discussion_comment dc
  on dc.discussion_id = dm.id
  left JOIN encounter_dos ed
    ON ed.encounter_id = e.id
   AND ed.process_id = mp.process_id and ed.is_active =1 
--    left join encounter_dos_standard_comment edsc
--    on edsc.encounter_dos_id = ed.id and edsc.process_id = mp.process_id and edsc.is_Active = 1 
--   left join encounter_dos_comment edc 
--     on edc.encounter_dos_id = ed.id 
--     and edc.process_id = mp.process_id and edc.is_active =1 

left join encounter_dos_evidence_map edem 
    on edem.encounter_dos_id = e.id and edem.is_active = 1 
left join document_evidence dos_de 
    on dos_de.id = edem.document_evidence_id and dos_de.is_active = 1 
  -- LEFT JOIN visit_type_mst vtm
  --   ON ed.visit_type_id = vtm.id
  left JOIN cm_code c
    ON c.encounter_id = mp.encounter_id
   AND c.process_id   = mp.process_id
   AND c.encounter_dos_id = ed.id
   and c.status = 'ACCEPTED' and c.is_active =1 

  left join cm_code_meat_evidence_map ccme 
    on ccme.cm_code_id = c.id 
    and ccme.meat_category ='T' and ccme.is_active =1 

  left join document_evidence de
  on de.id = ccme.document_evidence_id and de.is_active =1 

  LEFT JOIN cm_code_hcc_group_map v24
    ON v24.cm_code_id = c.id and v24.is_active =1 
  LEFT JOIN cm_code_cms_hcc_v28_group_map v28
    ON v28.cm_code_id = c.id and v28.is_active =1 
  LEFT JOIN cm_code_rxhcc_hcc_group_map rx
    ON rx.cm_code_id = c.id and rx.is_active =1 

    where e.is_Active = 1 

  

  GROUP BY
    e.id,
    ed.id,
    d.name,
    mp.updated_date,
    d.patient_identifier,
    e.patient_last_name,
    e.patient_first_name,
    e.patient_middle_name,
    e.patient_birth_date, -- For TIMESTAMPDIFF calculation
    ed.encounter_actual_period_start, -- For TIMESTAMPDIFF calculation
    e.patient_gender,
    ed.visit_type_id,
    ed.encounter_actual_period_end,
    ed.encounter_practitioner_identifier,
    ed.encounter_practitioner_first_name,
    ed.encounter_practitioner_last_name,
    ed.encounter_address_hospital_name,
    ed.encounter_address_text,
    ed.encounter_address_city,
    ed.encounter_address_postal_code,
    ed.encounter_address_state,
    c.id, -- The primary key for the diagnosis code
    v24.hcc_group_name,
    v28.cms_hcc_v28_group_name,
    rx.rxhcc_hcc_group_name
)

SELECT
    
    `File Name`, 
    `Completed On`,
    '' as `Claim ID`,
    '' as `Claim Type`,
    '' as `Provider Type`,
    `Member ID`,
    `MBI`,
    `Member Last Name`, 
    `Member First Name`, 
    `Member Middle Name`, 
    DOB,
    Age , 
    Gender ,
    '' as `Member Address 1`,
    '' as `Member Address 2`,
    '' as `Member City`,
    '' as `Member State`,
    '' as `Member Postal Code`,
    '' as `Visit Type`,
    `From DOS`, 
    `To DOS`,
    `Rendering Provider NPI ID`, 
    '' as `Rendering Provider Organization Name`,
    `Rendering Provider Name Last`,
    `Rendering Provider Name First`, 
    `Rendering Provider TIN`
    `Rendering Provider Address Line 1`, 
    `Rendering Provider Address Line 2`,
    `Rendering Provider City`,
    `Rendering Provider Address State Code`,
     `Rendering Provider Postal Code`, 
    `Comment Field` ,
     `Evidence Comment`,
    `Diag`,
    '' as `Add/Delete Indicator`,

    -- MAX(CASE WHEN v24_row = 1 THEN v24_code END)   AS v24_1,
    -- MAX(CASE WHEN v24_row = 2 THEN v24_code END)   AS v24_2,
    -- MAX(CASE WHEN v28_row = 1 THEN v28_code END)   AS v28_1,
    -- MAX(CASE WHEN v28_row = 2 THEN v28_code END)   AS v28_2,
    -- MAX(CASE WHEN rxhcc_row = 1 THEN rxhcc_code END) AS rxhcc_1,
    -- MAX(CASE WHEN rxhcc_row = 2 THEN rxhcc_code END) AS rxhcc_2,
    MAX(CASE WHEN v24_row = 1 THEN v24_code END)  AS `V24 HCC1`,
    MAX(CASE WHEN v24_row = 2 THEN v24_code END) AS `V24 HCC2`,
    MAX(CASE WHEN v28_row = 1 THEN v28_code END) AS `V28 HCC1`,
    MAX(CASE WHEN v28_row = 2 THEN v28_code END) AS `V28 HCC2`,
    MAX(CASE WHEN rxhcc_row = 1 THEN rxhcc_code END) AS `V08 RX HCC1`,
    MAX(CASE WHEN rxhcc_row = 2 THEN rxhcc_code END) AS `V08 RX HCC2`,
    '' AS `Results`,
    '' AS `HealthPlan ID`,
    '' AS `Revenue Code`,
    '' AS `Procedure Codes`,
    '' AS `Place of Service`,
    '' AS `Bill Type`,
    project_name AS `Project Name`,
    '' AS `Project ID/Code`

FROM dat
GROUP BY
    `File Name`, `Completed On`, `Member ID`,
    `Member Last Name`, `Member First Name`, `Member Middle Name`, Age , Gender ,
    `Visit Type`, `From DOS`, `To DOS`,
    `Rendering Provider NPI ID`, `Rendering Provider Name First`, `Rendering Provider Name Last`,
    `Rendering Provider Address Line 1`, `Rendering Provider Address Line 2`,
    `Rendering Provider City`, `Rendering Provider Postal Code`, `Rendering Provider Address State Code`,
    `Comment Field` , `Evidence Comment`,
    `Diag`

order by `File Name`;
