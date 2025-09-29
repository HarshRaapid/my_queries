DROP TABLE IF EXISTS output_report.chpw_op;

CREATE TABLE output_report.chpw_op AS
WITH dat AS (
    SELECT DISTINCT
        mp.project_name,
        e.id AS e_id,
        ed.id AS ed_id,
        d.name AS `File Name`,
        DATE(COALESCE(mp.last_coding_date, mp.updated_date)) AS `Completed On`,
        d.patient_identifier AS `Member ID`,
        e.patient_last_name AS `Member Last Name`,
        e.patient_first_name AS `Member First Name`,
        e.patient_middle_name AS `Member Middle Name`,
        e.patient_birth_date AS DOB,
        TIMESTAMPDIFF(YEAR, e.patient_birth_date, CURDATE()) AS Age,
        e.patient_gender AS Gender,
        ed.visit_type_id AS `Visit Type`,
        ed.encounter_actual_period_start AS `From DOS`,
        ed.encounter_actual_period_end AS `To DOS`,
        ed.encounter_practitioner_identifier AS `Rendering Provider NPI ID`,
        ed.encounter_practitioner_first_name AS `Rendering Provider Name First`,
        ed.encounter_practitioner_last_name AS `Rendering Provider Name Last`,
        ed.encounter_address_hospital_name AS `Rendering Provider Address Line 1`,
        ed.encounter_address_text AS `Rendering Provider Address Line 2`,
        ed.encounter_address_city AS `Rendering Provider City`,
        ed.encounter_address_postal_code AS `Rendering Provider Postal Code`,
        ed.encounter_address_state AS `Rendering Provider Address State Code`,
        c.comment AS 'Condition Comment',
        CASE WHEN c.is_suppressed = 1 THEN 'S' ELSE '' END AS 'Tag',
        CASE
            WHEN ed.id IS NULL THEN GROUP_CONCAT(dc.comments SEPARATOR ', ')
            ELSE GROUP_CONCAT(DISTINCT standard_comment SEPARATOR ', ')
        END AS 'Comment Field',
        GROUP_CONCAT(DISTINCT de.evidence_text SEPARATOR ', ') AS `Evidence Comment`,
        c.condition_code AS `Diag`,
        v24.hcc_group_name AS v24_code,
        ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY v24.hcc_group_name) AS v24_row,
        v28.cms_hcc_v28_group_name AS v28_code,
        ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY v28.cms_hcc_v28_group_name) AS v28_row,
        rx.rxhcc_hcc_group_name AS rxhcc_code,
        ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY rx.rxhcc_hcc_group_name) AS rxhcc_row,

        COALESCE(MIN(page_no.document_page_no), MIN(de.document_page_no)) AS page_no


        
    FROM
        encounter_mst e
    JOIN (
        SELECT
            esmv.encounter_id,
            esmv.process_id,
            esmv.updated_date,
            p.user_specific_comments,
            p.standard_comments,
            p.name AS project_name,
            esmv.last_coding_date
        FROM
            ra_audit_apigateway.encounter_status_map_view esmv
        JOIN
            ra_audit_apigateway.project_mst p ON p.id = esmv.project_id AND p.is_active = 1
        WHERE
            p.name = 'CHPW_2024_Calibration_Batch'
            AND esmv.encounter_status_id IN (8, 9)
            AND esmv.client_name = 'CHPW'
            AND esmv.is_active = 1
            AND esmv.process_id = 1
    ) mp ON e.id = mp.encounter_id
    JOIN
        document_mst d ON d.encounter_id = e.id AND d.is_active = 1
    LEFT JOIN
        discussion_mst dm ON dm.encounter_id = e.id AND dm.is_active = 1
    LEFT JOIN
        discussion_comment dc ON dc.discussion_id = dm.id
    LEFT JOIN
        encounter_dos ed ON ed.encounter_id = e.id AND ed.process_id = mp.process_id AND ed.is_active = 1
    LEFT JOIN
        encounter_dos_standard_comment edsc ON edsc.encounter_dos_id = ed.id AND edsc.is_Active = 1
    LEFT JOIN
        cm_code c ON c.encounter_id = mp.encounter_id AND c.process_id = mp.process_id AND c.encounter_dos_id = ed.id AND c.status = 'ACCEPTED' AND c.is_active = 1
    LEFT JOIN
        cm_code_meat_evidence_map ccme ON ccme.cm_code_id = c.id AND ccme.meat_category = 'T' AND ccme.is_active = 1
    LEFT JOIN
        document_evidence de ON de.id = ccme.document_evidence_id AND de.is_active = 1
    LEFT JOIN
        ra_audit_apigateway.cm_code_evidence_map ccem ON ccem.cm_code_id = c.id AND ccem.is_active = 1
    LEFT JOIN
        ra_audit_apigateway.document_evidence de1 ON de1.id = ccem.document_evidence_id AND de1.is_Active = 1
    LEFT JOIN
        ra_audit_apigateway.document_evidence_coordinates AS page_no ON page_no.document_evidence_id = de1.id AND page_no.is_active = 1
    LEFT JOIN
        cm_code_hcc_group_map v24 ON v24.cm_code_id = c.id AND v24.is_active = 1
    LEFT JOIN
        cm_code_cms_hcc_v28_group_map v28 ON v28.cm_code_id = c.id AND v28.is_active = 1
    LEFT JOIN
        cm_code_rxhcc_hcc_group_map rx ON rx.cm_code_id = c.id AND rx.is_active = 1
    WHERE
        e.is_Active = 1
    GROUP BY
        e.id, ed.id, d.name, mp.updated_date, d.patient_identifier, e.patient_last_name,
        e.patient_first_name, e.patient_middle_name, e.patient_birth_date,
        ed.encounter_actual_period_start, e.patient_gender, ed.visit_type_id,
        ed.encounter_actual_period_end, ed.encounter_practitioner_identifier,
        ed.encounter_practitioner_first_name, ed.encounter_practitioner_last_name,
        ed.encounter_address_hospital_name, ed.encounter_address_text,
        ed.encounter_address_city, ed.encounter_address_postal_code, ed.encounter_address_state,
        c.id, v24.hcc_group_name, v28.cms_hcc_v28_group_name, rx.rxhcc_hcc_group_name
)
SELECT
    `File Name`,
    `Completed On`,
    '' AS `Claim ID`,
    '' AS `Claim Type`,
    '' AS `Provider Type`,
    `Member ID`,
    '' AS `MBI`,
    `Member Last Name`,
    `Member First Name`,
    `Member Middle Name`,
    DOB,
    Age,
    Gender,
    '' AS `Member Address 1`,
    '' AS `Member Address 2`,
    '' AS `Member City`,
    '' AS `Member State`,
    '' AS `Member Postal Code`,
    '' AS `Visit Type`,
    `From DOS`,
    `To DOS`,
    `Rendering Provider NPI ID`,
    '' AS `Rendering Provider Organization Name`,
    `Rendering Provider Name Last`,
    `Rendering Provider Name First`,
    '' AS `Rendering Provider TIN`,
    `Rendering Provider Address Line 1`,
    `Rendering Provider Address Line 2`,
    `Rendering Provider City`,
    `Rendering Provider Address State Code`,
    `Rendering Provider Postal Code`,
    `Comment Field`,
    `Evidence Comment`,
    `Diag`,
    '' AS `Add/Delete Indicator`,
    MAX(CASE WHEN v24_row = 1 THEN v24_code END) AS `V24 HCC1`,
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
    '' AS `Project ID/Code` ,
    page_no ,
    Tag AS 'Suppression Tag' ,
    `Condition Comment`


FROM
    dat
GROUP BY
    `File Name`, `Completed On`, `Member ID`, `Member Last Name`, `Member First Name`,
    `Member Middle Name`, Age, Gender, `Visit Type`, `From DOS`, `To DOS`,
    `Rendering Provider NPI ID`, `Rendering Provider Name First`, `Rendering Provider Name Last`,
    `Rendering Provider Address Line 1`, `Rendering Provider Address Line 2`, `Rendering Provider City`,
    `Rendering Provider Postal Code`, `Rendering Provider Address State Code`, `Comment Field`,
    `Evidence Comment`, `Diag`
ORDER BY
    `File Name`;



mysql -hprod-coding-platform-service-flexible.mysql.database.azure.com -u uazureuser -p'72pY>2O5^@q3b>6' -e"use output_report; select * from chpw_op;" > chpw_level_1.csv


scp -i certificates/prod-coding-platform-workstation-key.pem ubuntu@10.7.64.5:/home/ubuntu/chpw_level_1.csv /home/harsh.k/chpw_level_1.csv

az ssh config --resource-group raapid-infra-rg --name raapid-infra-common-server-new --file ~/sshconfig --overwrite --yes-without-prompt
scp -F ~/sshconfig raapid-infra-rg-raapid-infra-common-server-new:~/chpw_level_1.csv ~/chpw_level_1.csv