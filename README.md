# HospitalClaims-ETL
Tento projekt predstavuje implementáciu ELT procesu v prostredí Snowflake a implementáciu návrhu dátového skladu so schémou Star Schema pre spracovanie zdravotníckych poistných dát. 
Projekt používa dataset Hospital Claims & Remits . Dataset obsahuje informácie o poistných nárokoch (claims), účtovaných položkách, úhradách poisťovni a zdravotníckych zariadeniach.

Cieľom projektu je transformovať komplexne dáta claimov do analyticky optimizovaneho dáta modelu, ktorý umožňuje jednoduchý proces analýzy dát.

---
## **1.	Úvod a popis zdrojových dát**
Zdrojové dáta pochádzajú z datasetu FinThrive Hospital Claims & Remmitance Data ktorý je dostupný [tu](https://app.snowflake.com/marketplace/listing/GZ1MJZ4HFKQ/finthrive-healthcare-hospital-claims-remits-data?search=healthcare&pricing=free&secondaryRegions=%5B%22ALL%22%5D). Cieľom je porozumieť
- štruktúru a výšku účtovaných nákladov (billed charges) ,
- Rozdiely medzi účtovanými (billed), uhradenými (paid) a upravenými (adjusted) sumami,
- Správanie poisťovni,
- využívanie zdravotnej starostlivosti v čase,
- porovnanie claimov.

### Prečo bol zvolený tento dataset
Dataset bol vybraný preto, že predstavuje dáta z oblasti zdravotníctva. Obsahuje viacero entít, časový rozmer a finančné metriky a umožňuje demonštrovať použitie analytických funkcií .

### Podporovaný biznis proces
Dáta podporujú biznis proces spracovania zdravotníckych nárokov (claims processing), ktorý zahŕňa:
- poskytovanie zdravotnej starostlivosti,
- fakturáciu výkonov,
- úhrady zo strany poisťovní,
- finančné vyrovnania a úpravy (adjustments).


Dataset obsahuje 8 tabuliek:
- `CLAIMDETAIL` – dáta claimov
- `CLAIMCHARGEDETAIL` – detailné položky služieb pre claimy
- `EOBDETAIL` – údaje o úhradách a úpravách
- `FACILITYDETAIL` – údaje o nemocniciach a zdravotníckych zariadeniach
- `CPTDETAIL` – informácie o CPT ( Current Procedural Terminology) kódoch
- `DIAGNOSISDETAIL` – diagnostické kódy 
- `PROCEDUREDETAIL` – dáta o procedúrach viazaných na claim
- `EOBMARKCODEDETAIL` – remarky a adjustment kódy k jednotlivým položkám

Výsledný dátový model umožňuje multidimenzionalnu analýzu dát.

---
### **1.1	Dátová architektúra**
Relačný model dát v datasete je znázornený na entitno-relačnom diagrame.
<p align="center">
  <img src="https://github.com/awwdwwp/HospitalClaims-ETL/blob/305b49c8adac3dccb01be998d50fec096530f98b/img/erd_schema.png" >
  <br>
  <em>Obrázok 1 ERD</em>
  <br>
  <img src="https://github.com/awwdwwp/HospitalClaims-ETL/blob/305b49c8adac3dccb01be998d50fec096530f98b/img/schema_erd_1.png" >
  <br>
  <em>Obrázok 2 ERD</em>
</p>

---
### **2.	Dimenzionálny model**
Pre tvorenie Star Schemy používajú sa 4 základne tabuľky datasetu:
1.	Tabuľka **`CLAIMDETAIL`** – *hlavný zdroj dát pre faktovú tabuľku.*
`CLAIMDETAIL` obsahuje klinické, administratívne aj finančne informácie.
Kľúčové atribúty:
- `CLAIMID` – indikátor claimu
- `PROVIDERID` – identifikátor poskytovateľa / zariadenia
- `PATIENT_KEY` – identifikátor pacienta
- `BILLEDDATE` – dátum vystavenia claimu
- `TOTALCHARGES`  - celková účtovaná suma
- `ESTAMTDUE` – odhadovaná suma na úhradu
- `ATTENDINGPHYSICIANNPI`, `RENDERINGPROVIDERNPI`, `TAXONOMY` – údaje o poskytovateľoch


2.	**`CLAIMCHARGEDETAIL`** – *detailne položky claimu*<br>

Kľúčové použite atribúty:
- `CLAIMID`  - identifikátor claimu
- `UNITS` – počet jednotiek
- `CHARGES` – suma za položku


3.	**`EOBDETAIL`** – údaje o finančných úhradách

Kľúčové použite atribúty:
- `CLAIMID` – identifikátor claimu
- `EOBPAYERID` – identifikátor poisťovne
- `EOBPAYERNAME` – názov poisťovne
- `PAIDAMOUNT` – suma zaplatená poisťovňou za cely claim alebo časť
- `TOTALADJUSTMENTAMOUNT` – celková suma finančných uprav 
- `LASTMODIFIEDDATE` – dátum poslednej úpravy


4.	**`FACILITYDETAIL`** – zdravotnícke zariadenia
   
Kľúčové použite atribúty:
- `PROVIDERID` – identifikátor zdravotníckeho zariadenia.

*V dátach rovnaký identifikátor sa používa pre poskytovateľa a zariadenie preto PROVIDERID je použitý ako primárny kľuč aj pre dim_facility aj pre dim_provider*
- `BEDSIZE` – počet lôžok nemocnice
- `HOSPITALTYPE` – typ nemocnice
- `TEACHINGSTATUS` – informácia o tom ci nemocnica je výučbová (akademická / nie akademická)
- `REGION` – geograficky región
- `GEOGRAPHICCLASSIFICATION` – klasifikácia lokality (urban / rural)
- `FACILITYZIP` – PSČ zariadenia 

---
## **Nepouzite tabulky a dovody ich nepouzitia**
**`CPTDETAIL`**

*Tabuľka obsahuje informácie o CPT kódoch ktoré popisujú zdravotne výkony na úrovni procedúr, ma klinicky fókus , údaje nie sú užitočne pre finančnú analýzu.*

**`DIAGNOSISDETAIL`**

*Tabuľka obsahuje len diagnostické kódy, numerické dáta ktoré sa nie používajú pre analýzu claimovych dát.*

**`PROCEDUREDETAIL`**

*Obsahuje len numerické dáta, identifikátory a procedurálny kód. Dáta tabuľky sú redundantne podlá iných tabuliek a vedu k zbytočnému nárastu objemu pri použití.*

**`EOBMARKCODEDETAIL`**

*Tabuľka obsahuje detailne remark a adjustment kódy a nie ma výhodu pre agregovanú analytiku. Dáta redukujú hodnoty paidamount, amount a majú príliš vysoku granularitu.*

--- 
<p align="center">
  <img src="https://github.com/awwdwwp/HospitalClaims-ETL/blob/305b49c8adac3dccb01be998d50fec096530f98b/img/star_schema.png" >
  <br>
  <em>Obrázok 3: Star Schema pre Hospital Claims</em>
  <br>
</p>

**Schéma hviezdy** obsahuje **1 tabuľku faktov** a **5 dimenzii**:
- **`1. dim_date`**
  
Vzťah k faktovej tabuľke: `fact_claims.date_id`

Zdroj: `claimdetail_staging`

Obsahuje údaje ako deň, mesiac, rok, kvartál, názvy dni a mesiacov

SCD type: `0`

Zdôvodnenie: Kalendárne udaj esu nemenne, nepotrebujú sledovanie historických zmien

- **`2.	dim_provider`**
  
Vzťah k faktovej tabuľke: `fact_claims.provider_id`

Zdroj: `claimdetail_staging`

Obsahuje identifikátor poskytovateľa, attending a rendering physician NPI, provider     taxonomy

SCD type: `1`

Zdôvodnenie: zmeny údajov predstavujú najmä opravy alebo aktualizácie.

- **`3.	dim_facility`**
  
Vzťah k faktovej tabuľke: `fact_claims.facility_id`

Zdroj: `facilitydetail_staging`

Obsahuje veľkosť nemocnice, typ , status a geografické dáta ( región, klasifikácia, zip kod)

SCD type: `0`

Zdôvodnenie: história sa nie sleduje, ak sa hodnota zmení, historické dáta claims sa nezmenia retrospektívne

- **`4.	dim_patient`**
  
Vzťah k faktovej tabuľke: `fact_claims.patient_id`

Zdroj: `claimdetail_staging`

Obsahuje id pacienta, typ a zdroj prijatia

SCD type: `0`

Zdôvodnenie: Dáta sú tonizovane a anonymizovane bez potreby sledovania historických zmien.

- **`5.	dim_payer`**
  
Vzťah k faktovej tabuľke: `fact_claims.payer_id`

Zdroj: `eobdetail_staging`

Obsahuje id payera, názov payera.

SCD type: `1`

Zdôvodnenie: zmena názvov payerov sa riešia prepísaním bez uchovávania histórie.



**`6. Faktová tabuľka`**
**fact_claims**:

Primárny kľuč:
- `fact_claim_id` – kľuč generovaný pomocou `ROW_NUMBER()`

Cudzie kľúče:
- `provider_id` – dim_provider
- `facility_id` – dim_facility
- `patient_id` – dim_patient
- `payer_id` – dim_payer
- `date_id` – dim_date

Hlavne metriky:
- `totalcharges` – celková suma za claim
- `estamtdue` – odhadovaná suma na úhradu
- `total_units` – celkový počet jednotiek zo charge detailov
- `total_paid_amount` – celková suma uhradená payerom
- `total_adjustment_amount` – celková suma uprav

Window functions:
- `SUM(paidamount) OVER (PARTITION BY claimid)` – Výpočet celkovej uhradenej sumy na úrovni claimu
- `SUM(totaladjustmentamount) OVER (PARTITION BY claimid)` – Výpočet celkovej hodnoty uprav na úrovni claimu
- `ROW_NUMBER() OVER (PARTITION BY claimid ORDER BY lastmodifieddate DESC)` – Vyber najnovšie verzie EOB záznamu pre každý claim

---
## **3. ELT proces v Snowflake**

**Extract:**
Údaje sú extrahovane zo schémy ISTG z databázy CLAIMS z datasetu Hospital Claims & Remits Data. 

Príklad kódu pre tvorbu staging tabuliek:
```sql
CREATE OR REPLACE TABLE claimdetail_staging AS
SELECT * FROM HOSPITAL_CLAIMS__REMITS_DATA.ISTG.CLAIMDETAIL;

CREATE OR REPLACE TABLE claimchargedetail_staging AS
SELECT * FROM HOSPITAL_CLAIMS__REMITS_DATA.ISTG.CLAIMCHARGEDETAIL;

CREATE OR REPLACE TABLE eobdetail_staging AS
SELECT * FROM HOSPITAL_CLAIMS__REMITS_DATA.ISTG.EOBDETAIL;

CREATE OR REPLACE TABLE facilitydetail_staging AS
SELECT * FROM HOSPITAL_CLAIMS__REMITS_DATA.ISTG.FACILITYDETAIL;
```
## Load & Transform

V tej časti ETL procesu sa vytvárajú a napĺňajú sa dimenzie dát a faktová tabuľka a kontroluje sa celostnosť a správnosť údajov.  

Príklad kódu:

```sql
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

```

Tato dimenzia je typu SCD 0 pretože dátumy sú nemenne a ich atribúty sa nemenia.  
Dátumy sa transformujú na jednotlivé komponenty (deň, týždeň, mesiac, kvartál).

Dim_provider ktorý obsahuje údaje o poskytovateľoch, je typu SCD 1, pretože pri zmene údajov o poskytovateľovi sa hodnoty prepisu bez uchovávania histórie. Deduplikácia sa vykonala pomocou `ROW_NUMBER()` a výberu najnovších záznamov podlá dátumu importu a vyúčtovania.  

Príklad kódu:  
```sql
REATE OR REPLACE TABLE dim_provider AS
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
WHERE rn = 1;

```

Dim_facility uchováva informácie o zariadeniach a je typu `SCD 0`. Chýbajúce hodnoty boli doplnene predvolenými hodnotami (napr. `'unknown'` alebo `0`).  

Príklad kódu:  
```sql
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

```

Dim_patient je dimenzia typu `SCD 0`, pretože tabuľka obsahuje anonymizovane identifikátory a pacienti sú sledovaní iba ako jedinečné záznamy bez histórie zmien. Hodnoty `NULL` sú nahradené default hodnotami.  

Dim_payer je typ `SCD 1`, pretože názvy platcov sa môžu meniť a aktuálna hodnota sa prepisuje pri zmene. Duplicity odstránene pomocou `ROW_NUMBER()` a výberu najnovších záznamov.  

Príklad kódu:  
```sql
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

```

Faktová tabuľka je pripojená na všetky dimenzie, obsahuje agregovane metriky a používa window functions `SUM() OVER` a `ROW_NUMBER()`, ktoré umožnili:  
- Agregovanie sumy za celý nárok  
- Vybrať najnovší záznam pre každý claim  
- Čistiť duplicity a zachovať konzistenciu medzi faktmi a dimenziami  

Príklad kódu:  
```sql
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

```

**Validácia Dát:**  
1. Kontrola počtu riadkov vo faktovej tabuľke a porovnanie so staging tabuľkou:
```sql
SELECT (SELECT COUNT(DISTINCT claimid) FROM claimdetail_staging) AS staging_claims,
(SELECT COUNT(*) FROM fact_claims) AS fact_rows;
```

2. Kontrola hodnôt NULL a predvolených hodnôt:  
```sql
SELECT
SUM(CASE WHEN provider_id = 0 THEN 1 ELSE 0 END) AS missing_provider,
SUM(CASE WHEN patient_id = 'unknown' THEN 1 ELSE 0 END) AS missing_patient,
SUM(CASE WHEN payer_id = 'unknown' THEN 1 ELSE 0 END) AS missing_payer,
SUM(CASE WHEN date_id = 0 THEN 1 ELSE 0 END) AS missing_date
FROM fact_claims;
```

3. Kontrola pôvodných dát:  
```sql
SELECT
SUM(CASE WHEN c.BILLEDDATE IS NULL THEN 1 ELSE 0 END) AS missing_date
FROM claimdetail_staging c;
```

Po úspešnom nahraní dát do tabuľky faktov a dimenzií boli staging tabuľky odstránené na optimalizáciu úložiska:  
```sql
DROP TABLE IF EXISTS CLAIMCHARGEDETAIL_STAGING;
DROP TABLE IF EXISTS CLAIMDETAIL_STAGING;
DROP TABLE IF EXISTS EOBDETAIL_STAGING;
DROP TABLE IF EXISTS FACILITYDETAIL_STAGING;`
```
---
## **4. Vizualizácia dát**
**`Graf 1:`** Regions ordered by number of charges
```sql
SELECT
    f.region,
    SUM(c.totalcharges) AS total_charges
FROM fact_claims c
INNER JOIN dim_facility f
    ON f.dim_facility_id = c.facility_id
WHERE f.region <> 'unknown'
GROUP BY f.region
ORDER BY total_charges DESC;
```
<p align="center">
  <img src="https://github.com/awwdwwp/HospitalClaims-ETL/blob/921a113f457c79c9f43db04313c96696568d2463/img/graf1.png" >
  <br>
  <em>Graf 1</em>
  <br>
</p>
Tento graf zobrazuje regióny podľa celkovej sumy vyúčtovaných poplatkov (`totalcharges`). Pomáha identifikovať, v ktorých regiónoch vzniká najvyššie množstvo nákladov. Môže slúžiť na strategické rozhodovanie a plánovanie.Z údajov vyplýva že najviac nákladov ma región `Southwest`


**`Graf 2:`** Number of claims by each admission type
```sql
SELECT 
    p.admission_type,
    COUNT(*) AS num_claims
FROM fact_claims f
INNER JOIN dim_patient p ON f.patient_id = p.dim_patient_id
GROUP BY p.admission_type
ORDER BY num_claims DESC;
```
<p align="center">
  <img src="https://github.com/awwdwwp/HospitalClaims-ETL/blob/921a113f457c79c9f43db04313c96696568d2463/img/graf2.png" >
  <br>
  <em>Graf 2</em>
  <br>
</p>

Tento graf ukazuje počet nárokov (`claims`) podľa typu prijatia pacienta. Umožňuje porovnať, aké typy prijatí sú najčastejšie .Z údajov vyplýva že najväčší počet nárokov ma typ 3 (`87`), a neznámy typ (`0`) ma až 53 nároky.

**`Graf 3:`** Total Units Billed by Quarter in 2022
```sql
SELECT 
    d.quarter,
    SUM(f.total_units) AS total_units
FROM fact_claims f
INNER JOIN dim_date d 
    ON f.date_id = d.dim_date_id
WHERE d.year = 2022
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;
```
<p align="center">
  <img src="https://github.com/awwdwwp/HospitalClaims-ETL/blob/921a113f457c79c9f43db04313c96696568d2463/img/graf3.png" >
  <br>
  <em>Graf 3</em>
  <br>
</p>

Tento graf vizualizuje celkový počet jednotiek (`total_units`) vyfakturovaných v jednotlivých kvartáloch roku 2022. Umožňuje sledovať sezónne trendy a identifikovať kvartály s vyšším využitím služieb.Tento trend naznačuje, že najväčší počet jednotiek bol v `3.` kvartáli a že trend bol rastúci od `1.` po `3.` kvartál a následne klesajúci od `3.` po `4.` kvartál.

**`Graf 4:`** Top 5 payers by number of claims
```sql
SELECT
    p.payer_name AS payer,
    COUNT(*) AS num_claims
FROM fact_claims f
INNER JOIN dim_payer p
    ON f.payer_id = p.dim_payer_id
GROUP BY p.payer_name
ORDER BY num_claims DESC
LIMIT 5;
```
<p align="center">
  <img src="https://github.com/awwdwwp/HospitalClaims-ETL/blob/921a113f457c79c9f43db04313c96696568d2463/img/graf4.png" >
  <br>
  <em>Graf 4</em>
  <br>
</p>

Graf zobrazuje 5 payerov s najvyšším počtom nárokov. Pomáha identifikovať, ktorí poistenia platia najviac nárokov a poskytuje prehľad o objeme spolupráce s jednotlivými payermi.Graf nám poskytuje informáciu, že RESEARCH MEDICARE AZ a MEDICARE A AND B WPS zdieľajú rovnaké miesto v top 5 s rovnakým počtom claimov a UHC COMMUNITY PLAN má najvyšší počet claimov

**`Graf 5:`** Top payers by amount paid
```sql
SELECT
    p.payer_name,
    SUM(f.total_paid_amount) AS total_paid
FROM fact_claims f
INNER JOIN dim_payer p ON f.payer_id = p.dim_payer_id
GROUP BY p.payer_name
ORDER BY total_paid DESC
LIMIT 10;
```
<p align="center">
  <img src="https://github.com/awwdwwp/HospitalClaims-ETL/blob/921a113f457c79c9f43db04313c96696568d2463/img/graf5.png" >
  <br>
  <em>Graf 5</em>
  <br>
</p>

Tento graf zobrazuje top payerov podľa celkovej sumy zaplatených nárokov. Je užitočný na identifikáciu, ktorí payeri prispievajú najviac k príjmom a poskytuje informácie pre finančné plánovanie a reporting. Dáta ukazujú, že MEDICARE je absolútnym lídrom v top 10 platcoch podľa vyplatenej sumy, pričom vyplatil takmer štvornásobok sumy v porovnaní s druhým miestom, ktorým je AARP HEALTHCARE OPTIONS.

**`Graf 6:`** Total Amount Paid vs Total Charges
```sql
SELECT
    totalcharges,
    total_paid_amount
FROM fact_claims
WHERE totalcharges > 0 AND total_paid_amount > 0;
```
<p align="center">
  <img src="https://github.com/awwdwwp/HospitalClaims-ETL/blob/921a113f457c79c9f43db04313c96696568d2463/img/graf6.png" >
  <br>
  <em>Graf 6</em>
  <br>
</p>

Tento graf porovnáva vyúčtované poplatky (`totalcharges`) a skutočne zaplatené sumy (`total_paid_amount`). Vizualizácia ukazuje prípadné rozdiely medzi nákladmi a úhradami. Os X zobrazuje hodnotu TOTALCHARGES a os Y zobrazuje hodnotu TOTAL_AMOUNT_PAID.Väčšina hodnôt má podobnú vyplatenú sumu, avšak niektoré vykazujú výrazný rozdiel medzi TOTALCHARGES a TOTAL_AMOUNT_PAID


**Dashboard umožňuje jednoduché na pochopenie interpretácie komplexných dát a je veľmi užitočný na trendy údajov, finančnú štatistiku a porovnávanie podľa numerických hodnôt a dátumov.**


---
## **Autor:** Andrii Bobonych
---
