
-- Graph 5: Top payers by amount paid
SELECT
p.payer_name,
SUM(f.total_paid_amount) AS total_paid

FROM fact_claims f
INNER JOIN dim_payer p ON f.payer_id = p.dim_payer_id
GROUP BY p.payer_name
ORDER BY total_paid DESC
LIMIT 10;

-- Graph 2: Number of claims by each admission type

SELECT 
p.admission_type,
COUNT(*) AS num_claims
FROM fact_claims f
INNER JOIN dim_patient p ON f.patient_id = p.dim_patient_id
GROUP BY p.admission_type
ORDER BY num_claims DESC;

-- Graph 3: Total Units Billed by Quarter in 2022
SELECT 
d.quarter,
SUM(f.total_units) AS total_units
FROM fact_claims f
INNER JOIN dim_date d 
ON f.date_id = d.dim_date_id
WHERE d.year = 2022
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;


-- Graph 4: Top 5 payers by number of claims
SELECT
p.payer_name AS payer,
COUNT(*) AS num_claims
FROM fact_claims f
INNER JOIN dim_payer p
ON f.payer_id = p.dim_payer_id
GROUP BY p.payer_name
ORDER BY num_claims DESC
LIMIT 5;

-- Graph 1: Regions ordered by number of charges

SELECT
f.region,
SUM(c.totalcharges) AS total_charges
FROM fact_claims c
INNER JOIN dim_facility f
ON f.dim_facility_id = c.facility_id
WHERE f.region <> 'unknown'
GROUP BY f.region
ORDER BY total_charges DESC;

-- Graph 6: Total Amount Paid and Total Charges Amount comparison
SELECT
totalcharges,
total_paid_amount,
FROM fact_claims
WHERE totalcharges > 0 AND total_paid_amount > 0;