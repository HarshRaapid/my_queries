mysql -h prod-coding-platform-service-flexible.mysql.database.azure.com -u uazureuser -p'72pY>2O5^@q3b>6'

drop table output_report.chpw_op;
use ra_audit_apigateway;
CREATE TABLE output_report.chpw_op AS
with chpw_op as (SELECT distinct
    e.project_id,
    p.name as proj,
    e.id as id,
    d.name AS file_name,
   (SELECT ee.user
     FROM encounter_status_map_view ee
    WHERE ee.encounter_id = e.id AND ee.process_id = 1 and ee.is_active=1) AS coded_by,
   (SELECT ee.user 
    FROM encounter_status_map_view ee
    WHERE ee.encounter_id = e.id AND ee.process_id = 2 and ee.is_active=1) AS audited_by,
    DATE(COALESCE(es.last_coding_date,es.updated_date)) AS completed_on,
    '' AS claim_id,
    d.patient_identifier member_id,   
    patient_first_name as first_name,
    patient_last_name as last_name,
    patient_middle_name as middle_name,
    e.patient_birth_date AS DOB,
    TIMESTAMPDIFF(YEAR, e.patient_birth_date, CURDATE()) AS Age,
    e.patient_gender AS Gender,
    v.name as Visit_Type,
    ed.encounter_actual_period_start AS From_DOS, 
    ed.encounter_actual_period_start dos,
    ed.dos_status,
    ed.encounter_address_hospital_name  hospital_name,
    ed.encounter_practitioner_identifier npi,
    ed.encounter_practitioner_first_name AS provider_first_name,
    ed.encounter_practitioner_last_name AS provider_last_name,
    COALESCE(
    GROUP_CONCAT(DISTINCT ec.comment SEPARATOR ', '), 
    GROUP_CONCAT( DISTINCT CASE 
        WHEN  esc.standard_comment  = 'Other/Enter your own text' 
        THEN ed.comment 
        ELSE esc.standard_comment END SEPARATOR ', ')
    ) AS 'DOS_COMMENT',
    c.comment as code_comment,
    GROUP_CONCAT(distinct dic.comments SEPARATOR ', ') as chart_comments,
    c.condition_code AS code,
    REPLACE(MIN(ch.hcc_group_name),'HCC','') AS `V24_HCC1`,
    REPLACE(MAX(ch.hcc_group_name),'HCC','') AS `V24_HCC2`,
    REPLACE(MIN(cv.cms_hcc_v28_group_name),'HCC','') AS `V28_HCC1`,
    REPLACE(MAX(cv.cms_hcc_v28_group_name),'HCC','') AS `V28_HCC2`,
    REPLACE(MIN(cr.rxhcc_hcc_group_name),'RXHCC','')  AS `V08_RX_HCC1`,
    REPLACE(MAX(cr.rxhcc_hcc_group_name),'RXHCC','') AS `V08_RX_HCC2`,
    GROUP_CONCAT(DISTINCT de1.evidence_text SEPARATOR ', ') as meat,
    COALESCE(min(page_no.document_page_no),min(de.document_page_no)) as page_no,
    case when c.status = 'ACCEPTED' THEN 'UNLINKED' ELSE NULL END AS  Results,
    case when c.is_suppressed then 'S' else NULL end as Tag 
FROM
    encounter_mst e
INNER JOIN
    document_mst d ON e.id = d.encounter_id
INNER JOIN
    encounter_status_map es ON es.encounter_id = e.id
LEFT JOIN
    user_details ud ON ud.id = es.user_details_id
LEFT JOIN 
    encounter_dos ed ON ed.encounter_id = e.id AND ed.is_Active = 1 AND ed.process_id = 3 and ed.status='Accepted'
LEFT JOIN
     ra_audit_apigateway.encounter_dos_comment ec on ec.encounter_dos_id =ed.id  and ec.encounter_id=e.id  and ec.is_Active = 1
LEFT JOIN  
    ra_audit_apigateway.visit_type_mst v on v.id = ed.visit_type_id
LEFT JOIN
    ra_audit_apigateway.cm_code c ON c.encounter_dos_id = ed.id AND c.is_active = 1 AND c.process_id = 3 and c.status = 'ACCEPTED'
LEFT JOIN
    ra_audit_apigateway.project_mst p ON e.project_id = p.id
LEFT JOIN 
    ra_audit_apigateway.cm_code_hcc_group_map as ch ON ch.cm_code_id = c.id and ch.is_Active = 1
LEFT JOIN 
    ra_audit_apigateway.cm_code_cms_hcc_v28_group_map as cv ON cv.cm_code_id = c.id and cv.is_Active = 1
LEFT JOIN 
    ra_audit_apigateway.cm_code_rxhcc_hcc_group_map as cr ON cr.cm_code_id = c.id and cr.is_Active = 1
LEFT JOIN 
     ra_audit_apigateway.cm_code_meat_evidence_map ccem1 on ccem1.cm_code_id=c.id AND ccem1.is_active=1 and ccem1.meat_category='T'
LEFT JOIN 
     ra_audit_apigateway.document_evidence de1 on de1.id=ccem1.document_evidence_id and de1.is_Active = 1
LEFT JOIN 
     ra_audit_apigateway.cm_code_evidence_map ccem on ccem.cm_code_id = c.id and ccem.is_active = 1
LEFT JOIN 
     ra_audit_apigateway.document_evidence de on de.id=ccem.document_evidence_id and de.is_Active = 1
LEFT JOIN
     ra_audit_apigateway.document_evidence_coordinates as page_no ON page_no.document_evidence_id  = de.id and page_no.is_active = 1
LEFT JOIN
     ra_audit_apigateway.encounter_dos_standard_comment esc on esc.encounter_dos_id =ed.id  and esc.encounter_id = e.id and esc.is_Active = 1
LEFT JOIN 
     ra_audit_apigateway.discussion_mst dim on dim.encounter_id = e.id and dim.is_active = 1
LEFT JOIN
     ra_audit_apigateway.discussion_comment dic on dic.discussion_id = dim.id and dic.is_active = 1
WHERE
    e.is_active = 1
    AND d.is_active = 1
    AND encounter_status_id IN (8,9)
    AND es.is_active = 1
    AND es.process_id = 2 and e.project_id  in (675 , 687 , 704 , 705 )
group by e.id, ed.encounter_actual_period_start,ed.encounter_practitioner_identifier,c.condition_code)


select 
-- proj as project_name,
file_name as `File Name`,
completed_on as `Completed On`,
'' AS `Claim ID`,
'' as `Claim Type`,
'' as `Provider Type`,
coded_by as `Reviewed By`,
audited_by as `Audited By`,


SUBSTRING_INDEX(file_name , '_' , 1 )  AS `Member ID`,
'' as MBI,

CASE WHEN last_name is NULL THEN UPPER(SUBSTRING_INDEX(first_name, ' ', -1)) ELSE UPPER(last_name) END as `Member Last Name`,
CASE WHEN last_name is NULL THEN UPPER(SUBSTRING_INDEX(first_name, ' ', 1)) ELSE UPPER(first_name) END as `Member First Name`,
'' as `Member Middle Name`,
DOB as `Member Date of Birth`,
Age as `Member Age`,
Gender,

'' as `Member Address 1`,
'' as `Member Address 2`,
'' as `Member City`,
'' as `Member State`,
'' as `Member Postal Code`,

Visit_Type AS `Visit Type`,
From_DOS AS `From DOS`,
From_DOS AS `To DOS`,
npi AS `Rendering Provider NPI ID`,
'' as `Rendering Provider Organization Name`,
UPPER(provider_last_name) AS `Rendering Provider Name Last`,
UPPER(provider_first_name) AS `Rendering Provider Name First`,
'' as `Rendering Provider TIN`,
'' as `Rendering Provider Address Line 1`,
'' as `Rendering Provider Address Line 2`,
'' as `Rendering Provider City`,
'' as `Rendering Provider Address State Code`,
'' as `Rendering Provider Postal Code`,

CASE WHEN From_DOS is NULL THEN chart_comments ELSE DOS_COMMENT END AS `Comment Field`,
CONCAT_WS(', ', code_comment, meat) as `Evidence Comments`,
CODE as Diag,
Tag as `Add/Delete Indicator`,

V24_HCC1 as `V24 HCC1`,
V24_HCC2 as `V24 HCC2`,
V28_HCC1 as `V28 HCC1`,
V28_HCC2 as `V28 HCC2`,
V08_RX_HCC1 as `V08 RX HCC1`,
V08_RX_HCC2 as `V08 RX HCC2`,

'' as Results,
'' as `HealthPlan ID`,
'' as `Revenue Code`,
'' as `Procedure Codes`,
'' as `Place of Service`,
'' as `Bill Type`,
proj as `Project Name`,
'' as `Project ID/Code`,
page_no as `Page No`,
is_dos_verified

from chpw_op;

mysql -hprod-coding-platform-service-flexible.mysql.database.azure.com -u uazureuser -p'72pY>2O5^@q3b>6' -e"use output_report; select * from sentara_op;" > sentara_6_03_8_03_8_02_5_02_op.csv


scp -i certificates/prod-coding-platform-workstation-key.pem ubuntu@10.7.64.5:/home/ubuntu/sentara_6_03_8_03_8_02_5_02_op.csv /home/harsh.k/sentara_6_03_8_03_8_02_5_02_op.csv

az ssh config --resource-group raapid-infra-rg --name raapid-infra-common-server-new --file ~/sshconfig --overwrite --yes-without-prompt
scp -F ~/sshconfig raapid-infra-rg-raapid-infra-common-server-new:~/sentara_6_03_8_03_8_02_5_02_op.csv ~/sentara_6_03_8_03_8_02_5_02_op.csv






('SHP_MA_4_03_MRR_2024DOS_fin_to Raapid',



select id from project_mst where name in('SHP_MA_5_02_MRR_2024DOS_fin_to Raapid', 'SHP_MA_6_03_MRR_2024DOS_fin_to Raapid')



select id from project_mst where name in 
('SHP_MA_6_01_MRR_2024DOS_fin_to Raapid' ,
'SHP_MA_6_03_MRR_2024DOS_fin_to Raapid' ,
'SHP_MA_8_02_MRR_2024DOS_fin_to Raapid' ,
'SHP_MA_8_03_MRR_2024DOS_fin_to Raapid')

6_01_6_03_8_02_8_03



mysql -hprod-coding-platform-service-flexible.mysql.database.azure.com -u uazureuser -p'72pY>2O5^@q3b>6' -e"use ra_audit_apigateway ; SELECT distinct  bcbs.* ,pm.name from ra_Audit_apigateway.final_op_bcbs bcbs left join ra_Audit_apigateway.document_mst dm on dm.patient_identifier =bcbs.MBI inner join encounter_mst em on em.id =dm.encounter_id  left join  project_mst pm  on pm.id =em.project_id where project_id in (650,682,690);;" > bcbs_final_op_256.csv


SELECT distinct  bcbs.* ,pm.name from ra_Audit_apigateway.final_op_bcbs bcbs left join ra_Audit_apigateway.document_mst dm on dm.patient_identifier =bcbs.MBI inner join encounter_mst em on em.id =dm.encounter_id  left join  project_mst pm  on pm.id =em.project_id where project_id in (650,682,690);


scp -i certificates/prod-coding-platform-workstation-key.pem ubuntu@10.7.64.5:/home/ubuntu/bcbs_final_op_256.csv /home/harsh.k/bcbs_final_op_256.csv


scp -F ~/sshconfig raapid-infra-rg-raapid-infra-common-server-new:~/bcbs_final_op_256.csv ~/bcbs_final_op_256.csv



select id from project_mst where name in  ( 'SHP_MA_8_02_MRR_2024DOS_fin_to Raapid', 'SHP_MA_8_03_MRR_2024DOS_fin_to Raapid' , 'SHP_MA_5_02_MRR_2024DOS_fin_to Raapid', 'SHP_MA_6_03_MRR_2024DOS_fin_to Raapid' )

675 |
| 687 |
| 704 |
| 705

(675 , 687 , 704 , 705 )

(675 , 687 , 694 , 696 ,  704 , 705)



sentara_5_02_6_03_7_01_7_03_8_02_8_03_f_op



select id from project_mst where name in 
('SHP_MA_7_04_MRR_2024DOS_fin_to Raapid',
'SHP_MA_8_04_MRR_2024DOS_fin_to Raapid',
'SHP_MA_9_01_MRR_2024DOS_fin_to Raapid',
'SHP_MA_9_02_MRR_2024DOS_fin_to Raapid',
'SHP_MA_9_03_MRR_2024DOS_fin_to Raapid',
'SHP_MA_9_04_MRR_2024DOS_fin_to Raapid')

(697 , 706 , 711 , 712 , 713 , 714)


sentara_7_8_9_level_1_op

sentara_7_04_8_04_9_01_9_02_9_03_9_04_level_1






select id from project_mst where name in 

