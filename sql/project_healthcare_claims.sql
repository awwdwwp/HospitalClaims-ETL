HOSPITAL_CLAIMS__REMITS_DATA.ISTG.CLAIMCHARGEDETAILUSE 
-- Prepojenie na Warehouse a databázu
WAREHOUSE CHEETAH_WH;
USE DATABASE CHEETAH_DB;

--Vytvorenie schémy
CREATE OR REPLACE SCHEMA Hospital_CLaims_DB;

USE SCHEMA  Hospital_CLaims_DB;

-- Vytvorenie tabuľky claimdetail (staging)
CREATE OR REPLACE TABLE claimdetail_staging
AS SELECT *
FROM HOSPITAL_CLAIMS__REMITS_DATA.ISTG.CLAIMDETAIL;

-- Kontrola dát
SELECT * FROM claimdetail_staging
LIMIT 10;

-- Vytvorenie tabuľky claimchargedetail (staging)
CREATE OR REPLACE TABLE claimchargedetail_staging
AS SELECT *
FROM HOSPITAL_CLAIMS__REMITS_DATA.ISTG.CLAIMCHARGEDETAIL;

DESCRIBE TABLE claimchargedetail_staging;

-- Vytvorenie tabuľky eobdetail (staging)
CREATE OR REPLACE TABLE eobdetail_staging
AS SELECT *
FROM HOSPITAL_CLAIMS__REMITS_DATA.ISTG.EOBDETAIL;

SELECT * FROM eobdetail_staging LIMIT 10;

-- Vytvorenie tabuľky facilitydetail (staging)
CREATE OR REPLACE TABLE facilitydetail_staging
AS SELECT *
FROM HOSPITAL_CLAIMS__REMITS_DATA.ISTG.FACILITYDETAIL;

-- Vytvorenie a naplnenie tabuľky dim_date 
CREATE OR REPLACE TABLE dim_date AS
WITH unique_dates AS (
SELECT DISTINCT CAST(BILLEDDATE AS DATE) AS bill_date
FROM claimdetail_staging
WHERE BILLEDDATE IS NOT NULL
) -- zoznam unikátnych dát
SELECT
ROW_NUMBER() OVER (ORDER BY bill_date) AS dim_date_id,
bill_date AS full_date,
DAY(bill_date) AS day,
DATE_PART(DAYOFWEEK,bill_date) AS weekday,
CASE DATE_PART(DAYOFWEEK, bill_date)
WHEN 1 THEN 'Monday'
WHEN 2 THEN 'Tuesday'
WHEN 3 THEN 'Wednesday'
WHEN 4 THEN 'Thursday'
WHEN 5 THEN 'Friday'
WHEN 6 THEN 'Saturday'
WHEN 0 THEN 'Sunday'
END AS weekday_name,
MONTH(bill_date) AS month,
CASE MONTH(bill_date)
WHEN 1 THEN 'January'
WHEN 2 THEN 'February'
WHEN 3 THEN 'March'
WHEN 4 THEN 'April'
WHEN 5 THEN 'May'
WHEN 6 THEN 'June'
WHEN 7 THEN 'July'
WHEN 8 THEN 'August'
WHEN 9 THEN 'September'
WHEN 10 THEN 'October'
WHEN 11 THEN 'November'
WHEN 12 THEN 'December'
END AS month_name,
YEAR(bill_date) AS year,
WEEK(bill_date) AS week,
QUARTER(bill_date) AS quarter
FROM unique_dates
;
-- Kontrola dát
SELECT * FROM dim_date LIMIT 10;

-- Vytvorenie a naplnenie tabuľky dim_provider 

CREATE OR REPLACE TABLE dim_provider AS
SELECT
dim_provider_id,
attending_npi,
rendering_npi,
provider_taxonomy
FROM (
SELECT
providerid AS dim_provider_id,
COALESCE(TRY_TO_NUMBER(attendingphysiciannpi), 0) AS attending_npi,
COALESCE(TRY_TO_NUMBER(renderingprovidernpi), 0) AS rendering_npi,
COALESCE(attendingphysiciantaxonomy, 'unknown') AS provider_taxonomy, -- Nahradenie null hodnotami 0 alebo 'unknown'
ROW_NUMBER() OVER (
PARTITION BY providerid
ORDER BY IMPORTDATE DESC, BILLEDDATE DESC
) AS rn -- Každý riadok s rovnakými id ma svoje číslo
FROM claimdetail_staging
WHERE providerid IS NOT NULL
)
WHERE rn = 1; -- Zabraňuje duplicite dát

SELECT dim_provider_id,
COUNT(*)
FROM dim_provider
GROUP BY dim_provider_id
HAVING COUNT(*) > 1;

SELECT * FROM dim_provider;

-- Vytvorenie a naplnenie tabuľky dim_facility
CREATE OR REPLACE TABLE dim_facility AS 
SELECT DISTINCT
providerid AS dim_facility_id,
bedsize,
hospitaltype,
teachingstatus,
region,
geographicclassification,
facilityzip
FROM facilitydetail_staging;

-- Nahradenie hodnôt null default hodnotami
UPDATE dim_facility
SET bedsize = 0 WHERE bedsize IS NULL;
UPDATE dim_facility
SET hospitaltype = 'unknown' WHERE hospitaltype IS NULL;
UPDATE dim_facility
SET teachingstatus = 'unknown' WHERE teachingstatus IS NULL;
UPDATE dim_facility SET REGION = 'unknown'
WHERE region IS NULL;
UPDATE dim_facility
SET geographicclassification = 'unknown' WHERE geographicclassification IS NULL;

--Kontrola dát
SELECT * FROM dim_facility LIMIT 10;
SELECT * FROM facilitydetail_staging LIMIT 10;

-- Vytvorenie a naplnenie tabuľky dim_patient
CREATE OR REPLACE TABLE dim_patient AS
SELECT DISTINCT
patient_key AS dim_patient_id,
COALESCE(uniquepatient_key, 'unknown') AS unique_patient_key,
COALESCE(TRY_TO_NUMBER(admissiontype), 0) AS admission_type,
COALESCE(TRY_TO_NUMBER(admissionsource), 0) AS admission_source -- Nahradenie hodnôt null default hodnotami
FROM claimdetail_staging;

-- Porovnávanie dát
SELECT * FROM dim_patient LIMIT 10;
SELECT * FROM claimdetail_staging LIMIT 6;

-- Vytvorenie a naplnenie tabuľky dim_payer 

CREATE OR REPLACE TABLE dim_payer AS
SELECT
dim_payer_id,
payer_name
FROM (
SELECT
eobpayerid AS dim_payer_id,
COALESCE(eobpayername, 'unknown') AS payer_name, -- nahradenie hodnôt null default hodnotami
ROW_NUMBER() OVER (
PARTITION BY eobpayerid
ORDER BY TRY_TO_TIMESTAMP(LASTMODIFIEDDATE) DESC
) AS rn -- Priradí poriadkové číslo každému riadku z rovnakými id
FROM eobdetail_staging
WHERE eobpayerid IS NOT NULL -- Zabraňuje duplicite dát
)
WHERE rn = 1;

SELECT dim_payer_id, COUNT(*) FROM dim_payer
GROUP BY dim_payer_id HAVING COUNT(*)>1;

-- Vytvorenie fact tabuľky
CREATE OR REPLACE TABLE fact_claims AS
WITH item_agg AS (
SELECT
claimid,
SUM(TRY_TO_NUMBER(units)) AS total_units
FROM claimchargedetail_staging
GROUP BY claimid
), -- CTE pre vypočítanie počtu jednotiek (units) pre claim
eob AS (
SELECT
claimid, 
eobpayerid, 
SUM(paidamount) OVER (PARTITION BY claimid) AS total_paid_amount,  -- Celková suma zaplatená za cely claim
SUM(totaladjustmentamount) OVER (PARTITION BY claimid) AS total_adjustment_amount,  -- Celková suma uprav za cely claim
ROW_NUMBER() OVER (PARTITION BY claimid
ORDER BY TRY_TO_TIMESTAMP(LASTMODIFIEDDATE) DESC) AS row_num -- Číslovanie riadkov v rámci claimu
FROM eobdetail_staging 
),  -- Spracovanie údajov o platbách
claims AS (
SELECT
claimid, 
providerid,
patient_key,
CAST(billeddate AS DATE) AS billed_date,
totalcharges,
estamtdue
FROM claimdetail_staging
) -- CTE pre spracovanie údajov z claimdetail_staging
SELECT
ROW_NUMBER() OVER (ORDER BY c.claimid) AS fact_claim_id, -- Unikátny ID
COALESCE(pr.dim_provider_id,0) AS provider_id, -- Prepojenie na dimenziu provider
COALESCE(f.dim_facility_id,0) AS facility_id, -- Prepojenie na dimenziu facility
COALESCE(p.dim_patient_id, 'unknown') AS patient_id, --Prepojenie na dimenziu pacienta (patient)
COALESCE(py.dim_payer_id,'unknown') AS payer_id, -- Prepojenie na dimenziu payer
COALESCE(d.dim_date_id,0) AS date_id, -- Prepojenie na dimenziu datumu (date)
c.billed_date,
c.totalcharges,
c.estamtdue,
i.total_units,
COALESCE(e.total_paid_amount, 0) AS total_paid_amount,
COALESCE(e.total_adjustment_amount, 0) AS total_adjustment_amount
FROM claims c
LEFT JOIN item_agg i ON c.claimid = i.claimid -- Prepojenie na základe claimid
LEFT JOIN eob e ON c.claimid = e.claimid
AND e.row_num = 1 -- 1 riadok pre claimid (najnovší)
LEFT JOIN dim_provider pr ON c.providerid = pr.dim_provider_id --Prepojenie na dimenziu provider na základe providerid
LEFT JOIN dim_facility f ON c.providerid = f.dim_facility_id -- Prepojenie na dimenziu facility
LEFT JOIN dim_patient p ON c.patient_key = p.dim_patient_id -- Prepojenie na zaklade patient_key
LEFT JOIN dim_payer py ON e.eobpayerid = py.dim_payer_id -- Prepojenie na zaklade eobpayerid
LEFT JOIN dim_date d ON c.billed_date = d.full_date; -- Prepojenie na zaklade datumu

-- Kontrola počtu riadkov
SELECT * FROM fact_claims LIMIT 10;
SELECT COUNT(*) FROM fact_claims;

SELECT eobpayerid
FROM eobdetail_staging;

SELECT COUNT(*) FROM fact_claims;

-- Porovnavanie počtu riadkov
SELECT (SELECT COUNT(DISTINCT claimid) FROM claimdetail_staging) AS staging_claims,
(SELECT COUNT(*) FROM fact_claims) AS fact_rows;

-- Kontrola riadkov s predchádzajúcimi hodnotami null
SELECT
SUM(CASE WHEN provider_id = 0 THEN 1 ELSE 0 END) AS missing_provider,
SUM(CASE WHEN patient_id = 'unknown' THEN 1 ELSE 0 END) AS missing_patient,
SUM(CASE WHEN payer_id = 'unknown' THEN 1 ELSE 0 END) AS missing_payer,
SUM(CASE WHEN date_id = 0 THEN 1 ELSE 0 END) AS missing_date
FROM fact_claims;

SELECT
SUM(CASE WHEN c.BILLEDDATE IS NULL THEN 1 ELSE 0 END) AS missing_date
FROM claimdetail_staging c;

-- DROP staging tables
DROP TABLE IF EXISTS CLAIMCHARGEDETAIL_STAGING;
DROP TABLE IF EXISTS CLAIMDETAIL_STAGING;
DROP TABLE IF EXISTS EOBDETAIL_STAGING;
DROP TABLE IF EXISTS FACILITYDETAIL_STAGING;
