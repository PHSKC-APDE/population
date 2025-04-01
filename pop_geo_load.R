devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/load_df_bcp.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/main/R/table_duplicate.R")

from_conn <- create_db_connection("hhsaw", interactive = F, prod = T)
to_conn <- create_db_connection("inthealth", interactive = F, prod = T)

table_duplicate_f(conn_from = from_conn, 
                  conn_to = to_conn, 
                  server_to = "inthealth", 
                  db_to = "inthealth_edw",
                  from_schema = "ref",
                  from_table = "pop_metadata_etl_log",
                  to_schema = "stg_reference",
                  confirm_tables = F,
                  delete_table = T)
table_duplicate_f(conn_from = from_conn, 
                  conn_to = to_conn, 
                  server_to = "inthealth", 
                  db_to = "inthealth_edw",
                  from_schema = "ref",
                  from_table = "pop_crosswalk",
                  to_schema = "stg_reference",
                  confirm_tables = F,
                  delete_table = T)
table_duplicate_f(conn_from = from_conn, 
                  conn_to = to_conn, 
                  server_to = "inthealth", 
                  db_to = "inthealth_edw",
                  from_schema = "ref",
                  from_table = "pop_hra_blk_crosswalk",
                  to_schema = "stg_reference",
                  confirm_tables = F,
                  delete_table = T)
table_duplicate_f(conn_from = from_conn, 
                  conn_to = to_conn, 
                  server_to = "inthealth", 
                  db_to = "inthealth_edw",
                  from_schema = "ref",
                  from_table = "pop_hra_crosswalk",
                  to_schema = "stg_reference",
                  confirm_tables = F,
                  delete_table = T)
table_duplicate_f(conn_from = from_conn, 
                  conn_to = to_conn, 
                  server_to = "inthealth", 
                  db_to = "inthealth_edw",
                  from_schema = "ref",
                  from_table = "pop_geoxwalks",
                  to_schema = "stg_reference",
                  confirm_tables = F,
                  delete_table = T)

if(1 == 1) {
  message(paste0("[", strptime(Sys.time(), format = "%Y-%m-%d %H:%M", tz = "GMT"), "] ...loading [pop_geo_blk]..."))
  DBI::dbExecute(to_conn, "
IF OBJECT_ID('tempdb..#etl_to_use') IS NOT NULL
  DROP TABLE #etl_to_use;
SELECT a.* 
INTO #etl_to_use
FROM stg_reference.pop_metadata_etl_log a
INNER JOIN (SELECT geo_type, geo_year, census_year, year, MAX(batch_date) AS batch_to_use 
	FROM stg_reference.pop_metadata_etl_log 
	WHERE load_ref_datetime IS NOT NULL AND delete_ref_datetime IS NULL
	GROUP BY geo_type, geo_year, census_year, year) b 
		ON a.geo_type = b.geo_type AND a.geo_year = b.geo_year AND a.census_year = b.census_year 
			AND a.year = b.year AND a.batch_date = b.batch_to_use
WHERE a.r_type = 97 AND a.census_year = 2020 AND (a.geo_year = 2020 OR a.geo_type = 'zip');
--------------------
-- BLOCK
--------------------
-- DELETE OLD BLK DATA
TRUNCATE TABLE stg_reference.pop_geo_blk
-- INSERT INTO GEO_BLK TABLE
INSERT INTO stg_reference.pop_geo_blk
(etl_batch_ID, 
geo_type, 
geo_scope, 
geo_year, 
census_year, 
year, 
r_type, 
geo_id, 
geo_id_tract, 
geo_id_blkgrp, 
age, 
age_5, 
age_11, 
age_20, 
age_100, 
gender, 
pop, 
race_wht, 
race_blk, 
race_aian, 
race_as, 
race_nhpi, 
race_hisp, 
rcode, 
r1r3, 
r2r4, 
fips_co, 
raw_agestr, 
raw_gender, 
raw_racemars, 
raw_hispanic)
SELECT 
a.id,
a.geo_type,
a.geo_scope, 
a.geo_year, 
a.census_year, 
a.year, 
a.r_type, 
b.geo_id,
SUBSTRING(b.geo_id, 1, 11), --geo_id_tract
SUBSTRING(b.geo_id, 1, 12), --geo_id_blkgrp
CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT), --age
ca5.new_value_num, --age_5
ca11.new_value_num, --age_11
ca20.new_value_num, --age_20
CASE
	WHEN CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) > 100
		THEN 100
		ELSE CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT)
	END, --age_100
cg.new_value_num, --gender
b.pop,
ISNULL(cr1.new_value_num, 0), --race_wht
ISNULL(cr2.new_value_num, 0), --race_blk
ISNULL(cr3.new_value_num, 0), --race_aian
ISNULL(cr4.new_value_num, 0), --race_as
ISNULL(cr5.new_value_num, 0), --race_nhpi
ISNULL(cr6.new_value_num, 0), --race_hisp
crc.new_value_num, --rcode
cr13.new_value_num, --r1r3
ISNULL(cr24h.new_value_num, cr24.new_value_num), --r2r4
CASE
	WHEN a.geo_scope = 'kps'
		THEN SUBSTRING(b.geo_id, 4, 2)
		ELSE NULL
	END, --fips_co
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic
FROM stg_reference.pop_metadata_etl_log a
INNER JOIN #etl_to_use z ON a.id = z.id
INNER JOIN stg_reference.pop b ON a.id = b.etl_batch_id AND SUBSTRING(b.geo_id, 4, 2) IN (33, 53, 61)
INNER JOIN stg_reference.pop_crosswalk ca5 ON ca5.old_column = 'age' AND ca5.new_column = 'age_5' AND CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) BETWEEN ca5.old_value_num_min AND ca5.old_value_num_max
INNER JOIN stg_reference.pop_crosswalk ca11 ON ca11.old_column = 'age' AND ca11.new_column = 'age_11' AND CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) BETWEEN ca11.old_value_num_min AND ca11.old_value_num_max
INNER JOIN stg_reference.pop_crosswalk ca20 ON ca20.old_column = 'age' AND ca20.new_column = 'age_20' AND CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) BETWEEN ca20.old_value_num_min AND ca20.old_value_num_max
INNER JOIN stg_reference.pop_crosswalk cg ON cg.old_column = 'raw_gender' AND cg.new_column = 'gender' AND b.raw_gender = cg.old_value_txt
INNER JOIN stg_reference.pop_crosswalk crc ON crc.old_column = 'raw_racemars' AND crc.new_column = 'rcode' AND b.raw_racemars = crc.old_value_txt AND a.r_type = crc.r_type
INNER JOIN stg_reference.pop_crosswalk cr13 ON cr13.old_column = 'rcode' AND cr13.new_column = 'r1r3' AND crc.new_value_num BETWEEN cr13.old_value_num_min AND cr13.old_value_num_max AND a.r_type = cr13.r_type
INNER JOIN stg_reference.pop_crosswalk cr24 ON cr24.old_column = 'rcode' AND cr24.new_column = 'r2r4' AND crc.new_value_num BETWEEN cr24.old_value_num_min AND cr24.old_value_num_max AND a.r_type = cr24.r_type
LEFT JOIN stg_reference.pop_crosswalk cr24h ON cr24h.old_column = 'raw_hispanic' AND cr24h.new_column = 'r2r4' AND b.raw_hispanic = cr24h.old_value_txt AND a.r_type = cr24h.r_type
LEFT JOIN stg_reference.pop_crosswalk cr1 ON cr1.old_column = 'raw_racemars' AND cr1.new_column = 'race_wht' AND b.raw_racemars = cr1.old_value_txt AND a.r_type = cr1.r_type
LEFT JOIN stg_reference.pop_crosswalk cr2 ON cr2.old_column = 'raw_racemars' AND cr2.new_column = 'race_blk' AND b.raw_racemars = cr2.old_value_txt AND a.r_type = cr2.r_type
LEFT JOIN stg_reference.pop_crosswalk cr3 ON cr3.old_column = 'raw_racemars' AND cr3.new_column = 'race_aian' AND b.raw_racemars = cr3.old_value_txt AND a.r_type = cr3.r_type
LEFT JOIN stg_reference.pop_crosswalk cr4 ON cr4.old_column = 'raw_racemars' AND cr4.new_column = 'race_as' AND b.raw_racemars = cr4.old_value_txt AND a.r_type = cr4.r_type
LEFT JOIN stg_reference.pop_crosswalk cr5 ON cr5.old_column = 'raw_racemars' AND cr5.new_column = 'race_nhpi' AND b.raw_racemars = cr5.old_value_txt AND a.r_type = cr5.r_type
LEFT JOIN stg_reference.pop_crosswalk cr6 ON cr6.old_column = 'raw_hispanic' AND cr6.new_column = 'race_hisp' AND b.raw_hispanic = cr6.old_value_txt
WHERE a.geo_type = 'blk'")
  
  message(paste0("[", strptime(Sys.time(), format = "%Y-%m-%d %H:%M", tz = "GMT"), "] ...loading [pop_geo_cou]..."))
  DBI::dbExecute(to_conn, "
IF OBJECT_ID('tempdb..#etl_to_use') IS NOT NULL
  DROP TABLE #etl_to_use;
SELECT a.* 
INTO #etl_to_use
FROM stg_reference.pop_metadata_etl_log a
INNER JOIN (SELECT geo_type, geo_year, census_year, year, MAX(batch_date) AS batch_to_use 
	FROM stg_reference.pop_metadata_etl_log 
	WHERE load_ref_datetime IS NOT NULL AND delete_ref_datetime IS NULL
	GROUP BY geo_type, geo_year, census_year, year) b 
		ON a.geo_type = b.geo_type AND a.geo_year = b.geo_year AND a.census_year = b.census_year 
			AND a.year = b.year AND a.batch_date = b.batch_to_use
WHERE a.r_type = 97 AND a.census_year = 2020 AND (a.geo_year = 2020 OR a.geo_type = 'zip');
--------------------
-- COUNTY
--------------------
-- DELETE OLD COU DATA
TRUNCATE TABLE stg_reference.pop_geo_cou
-- INSERT INTO GEO_COU TABLE
INSERT INTO stg_reference.pop_geo_cou
(etl_batch_ID, 
geo_type, 
geo_scope, 
geo_year, 
census_year, 
year, 
r_type, 
geo_id, 
age, 
age_5, 
age_11, 
age_20, 
age_100, 
gender, 
pop, 
race_wht, 
race_blk, 
race_aian, 
race_as, 
race_nhpi, 
race_hisp, 
rcode, 
r1r3, 
r2r4, 
fips_co, 
raw_agestr, 
raw_gender, 
raw_racemars, 
raw_hispanic)
SELECT 
a.id,
'cou',
'wa',
a.geo_year,
a.census_year,
a.year,
a.r_type,
SUBSTRING(b.geo_id, 1, 5),
CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT), --age
ca5.new_value_num, --age_5
ca11.new_value_num, --age_11
ca20.new_value_num, --age_20
CASE
	WHEN CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) > 100
		THEN 100
		ELSE CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT)
	END, --age_100
cg.new_value_num, --gender
SUM(b.pop),
ISNULL(cr1.new_value_num, 0), --race_wht
ISNULL(cr2.new_value_num, 0), --race_blk
ISNULL(cr3.new_value_num, 0), --race_aian
ISNULL(cr4.new_value_num, 0), --race_as
ISNULL(cr5.new_value_num, 0), --race_nhpi
ISNULL(cr6.new_value_num, 0), --race_hisp
crc.new_value_num, --rcode
cr13.new_value_num, --r1r3
ISNULL(cr24h.new_value_num, cr24.new_value_num), --r2r4
NULL, --fips_co
b.raw_agestr,
b.raw_gender,
b.raw_racemars,
b.raw_hispanic
FROM stg_reference.pop_metadata_etl_log a
INNER JOIN #etl_to_use z ON a.id = z.id
INNER JOIN stg_reference.pop b ON a.id = b.etl_batch_id
INNER JOIN stg_reference.pop_crosswalk ca5 ON ca5.old_column = 'age' AND ca5.new_column = 'age_5' AND CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) BETWEEN ca5.old_value_num_min AND ca5.old_value_num_max
INNER JOIN stg_reference.pop_crosswalk ca11 ON ca11.old_column = 'age' AND ca11.new_column = 'age_11' AND CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) BETWEEN ca11.old_value_num_min AND ca11.old_value_num_max
INNER JOIN stg_reference.pop_crosswalk ca20 ON ca20.old_column = 'age' AND ca20.new_column = 'age_20' AND CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) BETWEEN ca20.old_value_num_min AND ca20.old_value_num_max
INNER JOIN stg_reference.pop_crosswalk cg ON cg.old_column = 'raw_gender' AND cg.new_column = 'gender' AND b.raw_gender = cg.old_value_txt
INNER JOIN stg_reference.pop_crosswalk crc ON crc.old_column = 'raw_racemars' AND crc.new_column = 'rcode' AND b.raw_racemars = crc.old_value_txt AND a.r_type = crc.r_type
INNER JOIN stg_reference.pop_crosswalk cr13 ON cr13.old_column = 'rcode' AND cr13.new_column = 'r1r3' AND crc.new_value_num BETWEEN cr13.old_value_num_min AND cr13.old_value_num_max AND a.r_type = cr13.r_type
INNER JOIN stg_reference.pop_crosswalk cr24 ON cr24.old_column = 'rcode' AND cr24.new_column = 'r2r4' AND crc.new_value_num BETWEEN cr24.old_value_num_min AND cr24.old_value_num_max AND a.r_type = cr24.r_type
LEFT JOIN stg_reference.pop_crosswalk cr24h ON cr24h.old_column = 'raw_hispanic' AND cr24h.new_column = 'r2r4' AND b.raw_hispanic = cr24h.old_value_txt AND a.r_type = cr24h.r_type
LEFT JOIN stg_reference.pop_crosswalk cr1 ON cr1.old_column = 'raw_racemars' AND cr1.new_column = 'race_wht' AND b.raw_racemars = cr1.old_value_txt AND a.r_type = cr1.r_type
LEFT JOIN stg_reference.pop_crosswalk cr2 ON cr2.old_column = 'raw_racemars' AND cr2.new_column = 'race_blk' AND b.raw_racemars = cr2.old_value_txt AND a.r_type = cr2.r_type
LEFT JOIN stg_reference.pop_crosswalk cr3 ON cr3.old_column = 'raw_racemars' AND cr3.new_column = 'race_aian' AND b.raw_racemars = cr3.old_value_txt AND a.r_type = cr3.r_type
LEFT JOIN stg_reference.pop_crosswalk cr4 ON cr4.old_column = 'raw_racemars' AND cr4.new_column = 'race_as' AND b.raw_racemars = cr4.old_value_txt AND a.r_type = cr4.r_type
LEFT JOIN stg_reference.pop_crosswalk cr5 ON cr5.old_column = 'raw_racemars' AND cr5.new_column = 'race_nhpi' AND b.raw_racemars = cr5.old_value_txt AND a.r_type = cr5.r_type
LEFT JOIN stg_reference.pop_crosswalk cr6 ON cr6.old_column = 'raw_hispanic' AND cr6.new_column = 'race_hisp' AND b.raw_hispanic = cr6.old_value_txt
WHERE a.geo_type = 'blk'
GROUP BY
a.id,
a.geo_year,
a.census_year,
a.year,
a.r_type,
SUBSTRING(b.geo_id, 1, 5),
ca5.new_value_num,
ca11.new_value_num,
ca20.new_value_num,
cg.new_value_num, 
cr1.new_value_num,
cr2.new_value_num,
cr3.new_value_num,
cr4.new_value_num,
cr5.new_value_num,
cr6.new_value_num,
crc.new_value_num, 
cr13.new_value_num, 
cr24h.new_value_num, 
cr24.new_value_num,
b.raw_agestr,
b.raw_gender,
b.raw_racemars,
b.raw_hispanic")
  
  message(paste0("[", strptime(Sys.time(), format = "%Y-%m-%d %H:%M", tz = "GMT"), "] ...loading [pop_geo_lgd]..."))
  DBI::dbExecute(to_conn, "
IF OBJECT_ID('tempdb..#etl_to_use') IS NOT NULL
  DROP TABLE #etl_to_use;
SELECT a.* 
INTO #etl_to_use
FROM stg_reference.pop_metadata_etl_log a
INNER JOIN (SELECT geo_type, geo_year, census_year, year, MAX(batch_date) AS batch_to_use 
	FROM stg_reference.pop_metadata_etl_log 
	WHERE load_ref_datetime IS NOT NULL AND delete_ref_datetime IS NULL
	GROUP BY geo_type, geo_year, census_year, year) b 
		ON a.geo_type = b.geo_type AND a.geo_year = b.geo_year AND a.census_year = b.census_year 
			AND a.year = b.year AND a.batch_date = b.batch_to_use
WHERE a.r_type = 97 AND a.census_year = 2020 AND (a.geo_year = 2020 OR a.geo_type = 'zip');
--------------------
-- LEGISLATIVE DISTRICT
--------------------
-- DELETE OLD LGD DATA
TRUNCATE TABLE stg_reference.pop_geo_lgd
-- INSERT INTO GEO_LGD TABLE
INSERT INTO stg_reference.pop_geo_lgd
(etl_batch_ID, 
geo_type, 
geo_scope, 
geo_year, 
census_year, 
year, 
r_type, 
geo_id, 
age, 
age_5, 
age_11, 
age_20, 
age_100, 
gender, 
pop, 
race_wht, 
race_blk, 
race_aian, 
race_as, 
race_nhpi, 
race_hisp, 
rcode, 
r1r3, 
r2r4, 
raw_agestr, 
raw_gender, 
raw_racemars, 
raw_hispanic)
SELECT 
a.id,
a.geo_type,
a.geo_scope, 
a.geo_year, 
a.census_year, 
a.year, 
a.r_type, 
b.geo_id,
CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT), --age
ca5.new_value_num, --age_5
ca11.new_value_num, --age_11
ca20.new_value_num, --age_20
CASE
	WHEN CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) > 100
		THEN 100
		ELSE CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT)
	END, --age_100
cg.new_value_num, --gender
b.pop,
ISNULL(cr1.new_value_num, 0), --race_wht
ISNULL(cr2.new_value_num, 0), --race_blk
ISNULL(cr3.new_value_num, 0), --race_aian
ISNULL(cr4.new_value_num, 0), --race_as
ISNULL(cr5.new_value_num, 0), --race_nhpi
ISNULL(cr6.new_value_num, 0), --race_hisp
crc.new_value_num, --rcode
cr13.new_value_num, --r1r3
ISNULL(cr24h.new_value_num, cr24.new_value_num), --r2r4
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic
FROM stg_reference.pop_metadata_etl_log a
INNER JOIN #etl_to_use z ON a.id = z.id
INNER JOIN stg_reference.pop b ON a.id = b.etl_batch_id
INNER JOIN stg_reference.pop_crosswalk ca5 ON ca5.old_column = 'age' AND ca5.new_column = 'age_5' AND CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) BETWEEN ca5.old_value_num_min AND ca5.old_value_num_max
INNER JOIN stg_reference.pop_crosswalk ca11 ON ca11.old_column = 'age' AND ca11.new_column = 'age_11' AND CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) BETWEEN ca11.old_value_num_min AND ca11.old_value_num_max
INNER JOIN stg_reference.pop_crosswalk ca20 ON ca20.old_column = 'age' AND ca20.new_column = 'age_20' AND CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) BETWEEN ca20.old_value_num_min AND ca20.old_value_num_max
INNER JOIN stg_reference.pop_crosswalk cg ON cg.old_column = 'raw_gender' AND cg.new_column = 'gender' AND b.raw_gender = cg.old_value_txt
INNER JOIN stg_reference.pop_crosswalk crc ON crc.old_column = 'raw_racemars' AND crc.new_column = 'rcode' AND b.raw_racemars = crc.old_value_txt AND a.r_type = crc.r_type
INNER JOIN stg_reference.pop_crosswalk cr13 ON cr13.old_column = 'rcode' AND cr13.new_column = 'r1r3' AND crc.new_value_num BETWEEN cr13.old_value_num_min AND cr13.old_value_num_max AND a.r_type = cr13.r_type
INNER JOIN stg_reference.pop_crosswalk cr24 ON cr24.old_column = 'rcode' AND cr24.new_column = 'r2r4' AND crc.new_value_num BETWEEN cr24.old_value_num_min AND cr24.old_value_num_max AND a.r_type = cr24.r_type
LEFT JOIN stg_reference.pop_crosswalk cr24h ON cr24h.old_column = 'raw_hispanic' AND cr24h.new_column = 'r2r4' AND b.raw_hispanic = cr24h.old_value_txt AND a.r_type = cr24h.r_type
LEFT JOIN stg_reference.pop_crosswalk cr1 ON cr1.old_column = 'raw_racemars' AND cr1.new_column = 'race_wht' AND b.raw_racemars = cr1.old_value_txt AND a.r_type = cr1.r_type
LEFT JOIN stg_reference.pop_crosswalk cr2 ON cr2.old_column = 'raw_racemars' AND cr2.new_column = 'race_blk' AND b.raw_racemars = cr2.old_value_txt AND a.r_type = cr2.r_type
LEFT JOIN stg_reference.pop_crosswalk cr3 ON cr3.old_column = 'raw_racemars' AND cr3.new_column = 'race_aian' AND b.raw_racemars = cr3.old_value_txt AND a.r_type = cr3.r_type
LEFT JOIN stg_reference.pop_crosswalk cr4 ON cr4.old_column = 'raw_racemars' AND cr4.new_column = 'race_as' AND b.raw_racemars = cr4.old_value_txt AND a.r_type = cr4.r_type
LEFT JOIN stg_reference.pop_crosswalk cr5 ON cr5.old_column = 'raw_racemars' AND cr5.new_column = 'race_nhpi' AND b.raw_racemars = cr5.old_value_txt AND a.r_type = cr5.r_type
LEFT JOIN stg_reference.pop_crosswalk cr6 ON cr6.old_column = 'raw_hispanic' AND cr6.new_column = 'race_hisp' AND b.raw_hispanic = cr6.old_value_txt
WHERE a.geo_type = 'lgd'")
  
  message(paste0("[", strptime(Sys.time(), format = "%Y-%m-%d %H:%M", tz = "GMT"), "] ...loading [pop_geo_scd]..."))
  DBI::dbExecute(to_conn, "
IF OBJECT_ID('tempdb..#etl_to_use') IS NOT NULL
  DROP TABLE #etl_to_use;
SELECT a.* 
INTO #etl_to_use
FROM stg_reference.pop_metadata_etl_log a
INNER JOIN (SELECT geo_type, geo_year, census_year, year, MAX(batch_date) AS batch_to_use 
	FROM stg_reference.pop_metadata_etl_log 
	WHERE load_ref_datetime IS NOT NULL AND delete_ref_datetime IS NULL
	GROUP BY geo_type, geo_year, census_year, year) b 
		ON a.geo_type = b.geo_type AND a.geo_year = b.geo_year AND a.census_year = b.census_year 
			AND a.year = b.year AND a.batch_date = b.batch_to_use
WHERE a.r_type = 97 AND a.census_year = 2020 AND (a.geo_year = 2020 OR a.geo_type = 'zip');
--------------------
-- SCHOOL DISTRICT
--------------------
-- DELETE OLD SCD DATA
TRUNCATE TABLE stg_reference.pop_geo_scd
-- INSERT INTO GEO_SCD TABLE
INSERT INTO stg_reference.pop_geo_scd
(etl_batch_ID, 
geo_type, 
geo_scope, 
geo_year, 
census_year, 
year, 
r_type, 
geo_id, 
age, 
age_5, 
age_11, 
age_20, 
age_100, 
gender, 
pop, 
race_wht, 
race_blk, 
race_aian, 
race_as, 
race_nhpi, 
race_hisp, 
rcode, 
r1r3, 
r2r4, 
raw_agestr, 
raw_gender, 
raw_racemars, 
raw_hispanic)
SELECT 
a.id,
a.geo_type,
a.geo_scope, 
a.geo_year, 
a.census_year, 
a.year, 
a.r_type, 
b.geo_id,
CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT), --age
ca5.new_value_num, --age_5
ca11.new_value_num, --age_11
ca20.new_value_num, --age_20
CASE
	WHEN CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) > 100
		THEN 100
		ELSE CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT)
	END, --age_100
cg.new_value_num, --gender
b.pop,
ISNULL(cr1.new_value_num, 0), --race_wht
ISNULL(cr2.new_value_num, 0), --race_blk
ISNULL(cr3.new_value_num, 0), --race_aian
ISNULL(cr4.new_value_num, 0), --race_as
ISNULL(cr5.new_value_num, 0), --race_nhpi
ISNULL(cr6.new_value_num, 0), --race_hisp
crc.new_value_num, --rcode
cr13.new_value_num, --r1r3
ISNULL(cr24h.new_value_num, cr24.new_value_num), --r2r4
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic
FROM stg_reference.pop_metadata_etl_log a
INNER JOIN #etl_to_use z ON a.id = z.id
INNER JOIN stg_reference.pop b ON a.id = b.etl_batch_id
INNER JOIN stg_reference.pop_crosswalk ca5 ON ca5.old_column = 'age' AND ca5.new_column = 'age_5' AND CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) BETWEEN ca5.old_value_num_min AND ca5.old_value_num_max
INNER JOIN stg_reference.pop_crosswalk ca11 ON ca11.old_column = 'age' AND ca11.new_column = 'age_11' AND CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) BETWEEN ca11.old_value_num_min AND ca11.old_value_num_max
INNER JOIN stg_reference.pop_crosswalk ca20 ON ca20.old_column = 'age' AND ca20.new_column = 'age_20' AND CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) BETWEEN ca20.old_value_num_min AND ca20.old_value_num_max
INNER JOIN stg_reference.pop_crosswalk cg ON cg.old_column = 'raw_gender' AND cg.new_column = 'gender' AND b.raw_gender = cg.old_value_txt
INNER JOIN stg_reference.pop_crosswalk crc ON crc.old_column = 'raw_racemars' AND crc.new_column = 'rcode' AND b.raw_racemars = crc.old_value_txt AND a.r_type = crc.r_type
INNER JOIN stg_reference.pop_crosswalk cr13 ON cr13.old_column = 'rcode' AND cr13.new_column = 'r1r3' AND crc.new_value_num BETWEEN cr13.old_value_num_min AND cr13.old_value_num_max AND a.r_type = cr13.r_type
INNER JOIN stg_reference.pop_crosswalk cr24 ON cr24.old_column = 'rcode' AND cr24.new_column = 'r2r4' AND crc.new_value_num BETWEEN cr24.old_value_num_min AND cr24.old_value_num_max AND a.r_type = cr24.r_type
LEFT JOIN stg_reference.pop_crosswalk cr24h ON cr24h.old_column = 'raw_hispanic' AND cr24h.new_column = 'r2r4' AND b.raw_hispanic = cr24h.old_value_txt AND a.r_type = cr24h.r_type
LEFT JOIN stg_reference.pop_crosswalk cr1 ON cr1.old_column = 'raw_racemars' AND cr1.new_column = 'race_wht' AND b.raw_racemars = cr1.old_value_txt AND a.r_type = cr1.r_type
LEFT JOIN stg_reference.pop_crosswalk cr2 ON cr2.old_column = 'raw_racemars' AND cr2.new_column = 'race_blk' AND b.raw_racemars = cr2.old_value_txt AND a.r_type = cr2.r_type
LEFT JOIN stg_reference.pop_crosswalk cr3 ON cr3.old_column = 'raw_racemars' AND cr3.new_column = 'race_aian' AND b.raw_racemars = cr3.old_value_txt AND a.r_type = cr3.r_type
LEFT JOIN stg_reference.pop_crosswalk cr4 ON cr4.old_column = 'raw_racemars' AND cr4.new_column = 'race_as' AND b.raw_racemars = cr4.old_value_txt AND a.r_type = cr4.r_type
LEFT JOIN stg_reference.pop_crosswalk cr5 ON cr5.old_column = 'raw_racemars' AND cr5.new_column = 'race_nhpi' AND b.raw_racemars = cr5.old_value_txt AND a.r_type = cr5.r_type
LEFT JOIN stg_reference.pop_crosswalk cr6 ON cr6.old_column = 'raw_hispanic' AND cr6.new_column = 'race_hisp' AND b.raw_hispanic = cr6.old_value_txt
WHERE a.geo_type = 'scd'")
  
  message(paste0("[", strptime(Sys.time(), format = "%Y-%m-%d %H:%M", tz = "GMT"), "] ...loading [pop_geo_zip]..."))
  DBI::dbExecute(to_conn, "
IF OBJECT_ID('tempdb..#etl_to_use') IS NOT NULL
  DROP TABLE #etl_to_use;
SELECT a.* 
INTO #etl_to_use
FROM stg_reference.pop_metadata_etl_log a
INNER JOIN (SELECT geo_type, geo_year, census_year, year, MAX(batch_date) AS batch_to_use 
	FROM stg_reference.pop_metadata_etl_log 
	WHERE load_ref_datetime IS NOT NULL AND delete_ref_datetime IS NULL
	GROUP BY geo_type, geo_year, census_year, year) b 
		ON a.geo_type = b.geo_type AND a.geo_year = b.geo_year AND a.census_year = b.census_year 
			AND a.year = b.year AND a.batch_date = b.batch_to_use
WHERE a.r_type = 97 AND a.census_year = 2020 AND (a.geo_year = 2020 OR a.geo_type = 'zip');
--------------------
-- ZIP CODE
--------------------
-- DELETE OLD ZIP DATA
TRUNCATE TABLE stg_reference.pop_geo_zip
-- INSERT INTO GEO_ZIP TABLE
INSERT INTO stg_reference.pop_geo_zip
(etl_batch_ID, 
geo_type, 
geo_scope, 
geo_year, 
census_year, 
year, 
r_type, 
geo_id, 
age, 
age_5, 
age_11, 
age_20, 
age_100, 
gender, 
pop, 
race_wht, 
race_blk, 
race_aian, 
race_as, 
race_nhpi, 
race_hisp, 
rcode, 
r1r3, 
r2r4, 
raw_agestr, 
raw_gender, 
raw_racemars, 
raw_hispanic)
SELECT 
a.id,
a.geo_type,
a.geo_scope, 
a.geo_year, 
a.census_year, 
a.year, 
a.r_type, 
b.geo_id,
CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT), --age
ca5.new_value_num, --age_5
ca11.new_value_num, --age_11
ca20.new_value_num, --age_20
CASE
	WHEN CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) > 100
		THEN 100
		ELSE CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT)
	END, --age_100
cg.new_value_num, --gender
b.pop,
ISNULL(cr1.new_value_num, 0), --race_wht
ISNULL(cr2.new_value_num, 0), --race_blk
ISNULL(cr3.new_value_num, 0), --race_aian
ISNULL(cr4.new_value_num, 0), --race_as
ISNULL(cr5.new_value_num, 0), --race_nhpi
ISNULL(cr6.new_value_num, 0), --race_hisp
crc.new_value_num, --rcode
cr13.new_value_num, --r1r3
ISNULL(cr24h.new_value_num, cr24.new_value_num), --r2r4
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic
FROM stg_reference.pop_metadata_etl_log a
INNER JOIN #etl_to_use z ON a.id = z.id
INNER JOIN stg_reference.pop b ON a.id = b.etl_batch_id
INNER JOIN stg_reference.pop_crosswalk ca5 ON ca5.old_column = 'age' AND ca5.new_column = 'age_5' AND CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) BETWEEN ca5.old_value_num_min AND ca5.old_value_num_max
INNER JOIN stg_reference.pop_crosswalk ca11 ON ca11.old_column = 'age' AND ca11.new_column = 'age_11' AND CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) BETWEEN ca11.old_value_num_min AND ca11.old_value_num_max
INNER JOIN stg_reference.pop_crosswalk ca20 ON ca20.old_column = 'age' AND ca20.new_column = 'age_20' AND CAST(SUBSTRING(b.raw_agestr, 1, 3) AS SMALLINT) BETWEEN ca20.old_value_num_min AND ca20.old_value_num_max
INNER JOIN stg_reference.pop_crosswalk cg ON cg.old_column = 'raw_gender' AND cg.new_column = 'gender' AND b.raw_gender = cg.old_value_txt
INNER JOIN stg_reference.pop_crosswalk crc ON crc.old_column = 'raw_racemars' AND crc.new_column = 'rcode' AND b.raw_racemars = crc.old_value_txt AND a.r_type = crc.r_type
INNER JOIN stg_reference.pop_crosswalk cr13 ON cr13.old_column = 'rcode' AND cr13.new_column = 'r1r3' AND crc.new_value_num BETWEEN cr13.old_value_num_min AND cr13.old_value_num_max AND a.r_type = cr13.r_type
INNER JOIN stg_reference.pop_crosswalk cr24 ON cr24.old_column = 'rcode' AND cr24.new_column = 'r2r4' AND crc.new_value_num BETWEEN cr24.old_value_num_min AND cr24.old_value_num_max AND a.r_type = cr24.r_type
LEFT JOIN stg_reference.pop_crosswalk cr24h ON cr24h.old_column = 'raw_hispanic' AND cr24h.new_column = 'r2r4' AND b.raw_hispanic = cr24h.old_value_txt AND a.r_type = cr24h.r_type
LEFT JOIN stg_reference.pop_crosswalk cr1 ON cr1.old_column = 'raw_racemars' AND cr1.new_column = 'race_wht' AND b.raw_racemars = cr1.old_value_txt AND a.r_type = cr1.r_type
LEFT JOIN stg_reference.pop_crosswalk cr2 ON cr2.old_column = 'raw_racemars' AND cr2.new_column = 'race_blk' AND b.raw_racemars = cr2.old_value_txt AND a.r_type = cr2.r_type
LEFT JOIN stg_reference.pop_crosswalk cr3 ON cr3.old_column = 'raw_racemars' AND cr3.new_column = 'race_aian' AND b.raw_racemars = cr3.old_value_txt AND a.r_type = cr3.r_type
LEFT JOIN stg_reference.pop_crosswalk cr4 ON cr4.old_column = 'raw_racemars' AND cr4.new_column = 'race_as' AND b.raw_racemars = cr4.old_value_txt AND a.r_type = cr4.r_type
LEFT JOIN stg_reference.pop_crosswalk cr5 ON cr5.old_column = 'raw_racemars' AND cr5.new_column = 'race_nhpi' AND b.raw_racemars = cr5.old_value_txt AND a.r_type = cr5.r_type
LEFT JOIN stg_reference.pop_crosswalk cr6 ON cr6.old_column = 'raw_hispanic' AND cr6.new_column = 'race_hisp' AND b.raw_hispanic = cr6.old_value_txt
WHERE a.geo_type = 'zip'")
  
  message(paste0("[", strptime(Sys.time(), format = "%Y-%m-%d %H:%M", tz = "GMT"), "] ...loading [pop_geo_hra]..."))
  DBI::dbExecute(to_conn, "
--------------------
-- HRA
--------------------
TRUNCATE TABLE stg_reference.pop_geo_hra
INSERT INTO stg_reference.pop_geo_hra
(etl_batch_ID, 
geo_type, 
geo_scope, 
geo_year, 
census_year, 
year, 
r_type, 
geo_id, 
age, 
age_5, 
age_11, 
age_20, 
age_100, 
gender, 
pop, 
race_wht, 
race_blk, 
race_aian, 
race_as, 
race_nhpi, 
race_hisp, 
rcode, 
r1r3, 
r2r4, 
fips_co, 
raw_agestr, 
raw_gender, 
raw_racemars, 
raw_hispanic)
SELECT 
b.etl_batch_id,
'hra',
'kc',
x.hra_census_year,
b.census_year,
b.year,
b.r_type,
x.hra_geo_id,
b.age, 
b.age_5, 
b.age_11, 
b.age_20, 
b.age_100, 
b.gender, 
SUM(b.pop), 
b.race_wht, 
b.race_blk, 
b.race_aian, 
b.race_as, 
b.race_nhpi, 
b.race_hisp, 
b.rcode, 
b.r1r3, 
b.r2r4, 
b.fips_co, 
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic
FROM stg_reference.pop_geo_blk b 
INNER JOIN stg_reference.pop_hra_blk_crosswalk x ON b.census_year = x.blk_census_year AND b.geo_id = x.blk_geo_id
GROUP BY 
b.etl_batch_id,
x.hra_census_year,
b.census_year,
b.year,
b.r_type,
x.hra_geo_id,
b.age, 
b.age_5, 
b.age_11, 
b.age_20, 
b.age_100, 
b.gender, 
b.race_wht, 
b.race_blk, 
b.race_aian, 
b.race_as, 
b.race_nhpi, 
b.race_hisp, 
b.rcode, 
b.r1r3, 
b.r2r4, 
b.fips_co, 
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic")
  
  message(paste0("[", strptime(Sys.time(), format = "%Y-%m-%d %H:%M", tz = "GMT"), "] ...loading [pop_geo_reg]..."))
  DBI::dbExecute(to_conn, "
--------------------
-- REGION
--------------------
TRUNCATE TABLE stg_reference.pop_geo_reg
INSERT INTO stg_reference.pop_geo_reg
(etl_batch_ID, 
geo_type, 
geo_scope, 
geo_year, 
census_year, 
year, 
r_type, 
geo_id, 
age, 
age_5, 
age_11, 
age_20, 
age_100, 
gender, 
pop, 
race_wht, 
race_blk, 
race_aian, 
race_as, 
race_nhpi, 
race_hisp, 
rcode, 
r1r3, 
r2r4, 
fips_co, 
raw_agestr, 
raw_gender, 
raw_racemars, 
raw_hispanic)
SELECT 
b.etl_batch_id,
'reg',
b.geo_scope,
b.geo_year,
b.census_year,
b.year,
b.r_type,
x.reg_geo_id,
b.age, 
b.age_5, 
b.age_11, 
b.age_20, 
b.age_100, 
b.gender, 
SUM(b.pop), 
b.race_wht, 
b.race_blk, 
b.race_aian, 
b.race_as, 
b.race_nhpi, 
b.race_hisp, 
b.rcode, 
b.r1r3, 
b.r2r4, 
b.fips_co, 
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic
FROM stg_reference.pop_geo_hra b 
INNER JOIN stg_reference.pop_hra_crosswalk x ON b.geo_year = x.census_year AND b.geo_id = x.hra_geo_id
GROUP BY 
b.etl_batch_id,
b.geo_scope,
b.geo_year,
b.census_year,
b.year,
b.r_type,
x.reg_geo_id,
b.age, 
b.age_5, 
b.age_11, 
b.age_20, 
b.age_100, 
b.gender, 
b.race_wht, 
b.race_blk, 
b.race_aian, 
b.race_as, 
b.race_nhpi, 
b.race_hisp, 
b.rcode, 
b.r1r3, 
b.r2r4, 
b.fips_co, 
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic")
  
  message(paste0("[", strptime(Sys.time(), format = "%Y-%m-%d %H:%M", tz = "GMT"), "] ...loading [pop_geo_CCL]..."))
  DBI::dbExecute(to_conn, "
--------------------
-- CCL
--------------------
TRUNCATE TABLE stg_reference.pop_geo_CCL
INSERT INTO stg_reference.pop_geo_CCL
(etl_batch_ID, 
geo_type, 
geo_scope, 
geo_year, 
census_year, 
year, 
r_type, 
geo_id, 
age, 
age_5, 
age_11, 
age_20, 
age_100, 
gender, 
pop, 
race_wht, 
race_blk, 
race_aian, 
race_as, 
race_nhpi, 
race_hisp, 
rcode, 
r1r3, 
r2r4, 
fips_co, 
raw_agestr, 
raw_gender, 
raw_racemars, 
raw_hispanic)
SELECT 
b.etl_batch_id,
'CCL',
'kc',
x.geo_year,
x.census_year,
b.year,
b.r_type,
x.target_id,
b.age, 
b.age_5, 
b.age_11, 
b.age_20, 
b.age_100, 
b.gender, 
SUM(b.pop * x.proportion), 
b.race_wht, 
b.race_blk, 
b.race_aian, 
b.race_as, 
b.race_nhpi, 
b.race_hisp, 
b.rcode, 
b.r1r3, 
b.r2r4, 
b.fips_co, 
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic
FROM stg_reference.pop_geo_blk b 
INNER JOIN stg_reference.pop_geoxwalks x ON b.census_year = x.census_year AND b.geo_year = x.geo_year AND b.geo_id = x.geo_id
WHERE x.target_type = 'CCL'
GROUP BY 
b.etl_batch_id,
x.geo_year,
x.census_year,
b.year,
b.r_type,
x.target_id,
b.age, 
b.age_5, 
b.age_11, 
b.age_20, 
b.age_100, 
b.gender, 
b.race_wht, 
b.race_blk, 
b.race_aian, 
b.race_as, 
b.race_nhpi, 
b.race_hisp, 
b.rcode, 
b.r1r3, 
b.r2r4, 
b.fips_co, 
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic")
  
  message(paste0("[", strptime(Sys.time(), format = "%Y-%m-%d %H:%M", tz = "GMT"), "] ...loading [pop_geo_CSA]..."))
  DBI::dbExecute(to_conn, "
--------------------
-- CSA
--------------------
TRUNCATE TABLE stg_reference.pop_geo_CSA
INSERT INTO stg_reference.pop_geo_CSA
(etl_batch_ID, 
geo_type, 
geo_scope, 
geo_year, 
census_year, 
year, 
r_type, 
geo_id, 
age, 
age_5, 
age_11, 
age_20, 
age_100, 
gender, 
pop, 
race_wht, 
race_blk, 
race_aian, 
race_as, 
race_nhpi, 
race_hisp, 
rcode, 
r1r3, 
r2r4, 
fips_co, 
raw_agestr, 
raw_gender, 
raw_racemars, 
raw_hispanic)
SELECT 
b.etl_batch_id,
'CSA',
'kc',
x.geo_year,
x.census_year,
b.year,
b.r_type,
x.target_id,
b.age, 
b.age_5, 
b.age_11, 
b.age_20, 
b.age_100, 
b.gender, 
SUM(b.pop * x.proportion), 
b.race_wht, 
b.race_blk, 
b.race_aian, 
b.race_as, 
b.race_nhpi, 
b.race_hisp, 
b.rcode, 
b.r1r3, 
b.r2r4, 
b.fips_co, 
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic
FROM stg_reference.pop_geo_blk b 
INNER JOIN stg_reference.pop_geoxwalks x ON b.census_year = x.census_year AND b.geo_year = x.geo_year AND b.geo_id = x.geo_id
WHERE x.target_type = 'CSA'
GROUP BY 
b.etl_batch_id,
x.geo_year,
x.census_year,
b.year,
b.r_type,
x.target_id,
b.age, 
b.age_5, 
b.age_11, 
b.age_20, 
b.age_100, 
b.gender, 
b.race_wht, 
b.race_blk, 
b.race_aian, 
b.race_as, 
b.race_nhpi, 
b.race_hisp, 
b.rcode, 
b.r1r3, 
b.r2r4, 
b.fips_co, 
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic")
  
  message(paste0("[", strptime(Sys.time(), format = "%Y-%m-%d %H:%M", tz = "GMT"), "] ...loading [pop_geo_Inc_uninc]..."))
  DBI::dbExecute(to_conn, "
--------------------
-- Inc_Uninc
--------------------
TRUNCATE TABLE stg_reference.pop_geo_Inc_Uninc
INSERT INTO stg_reference.pop_geo_Inc_Uninc
(etl_batch_ID, 
geo_type, 
geo_scope, 
geo_year, 
census_year, 
year, 
r_type, 
geo_id, 
age, 
age_5, 
age_11, 
age_20, 
age_100, 
gender, 
pop, 
race_wht, 
race_blk, 
race_aian, 
race_as, 
race_nhpi, 
race_hisp, 
rcode, 
r1r3, 
r2r4, 
fips_co, 
raw_agestr, 
raw_gender, 
raw_racemars, 
raw_hispanic)
SELECT 
b.etl_batch_id,
'Inc_Uninc',
'kc',
x.geo_year,
x.census_year,
b.year,
b.r_type,
x.target_id,
b.age, 
b.age_5, 
b.age_11, 
b.age_20, 
b.age_100, 
b.gender, 
SUM(b.pop * x.proportion), 
b.race_wht, 
b.race_blk, 
b.race_aian, 
b.race_as, 
b.race_nhpi, 
b.race_hisp, 
b.rcode, 
b.r1r3, 
b.r2r4, 
b.fips_co, 
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic
FROM stg_reference.pop_geo_blk b 
INNER JOIN stg_reference.pop_geoxwalks x ON b.census_year = x.census_year AND b.geo_year = x.geo_year AND b.geo_id = x.geo_id
WHERE x.target_type = 'Inc_Uninc'
GROUP BY 
b.etl_batch_id,
x.geo_year,
x.census_year,
b.year,
b.r_type,
x.target_id,
b.age, 
b.age_5, 
b.age_11, 
b.age_20, 
b.age_100, 
b.gender, 
b.race_wht, 
b.race_blk, 
b.race_aian, 
b.race_as, 
b.race_nhpi, 
b.race_hisp, 
b.rcode, 
b.r1r3, 
b.r2r4, 
b.fips_co, 
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic")
  
  message(paste0("[", strptime(Sys.time(), format = "%Y-%m-%d %H:%M", tz = "GMT"), "] ...loading [pop_geo_kccd]..."))
  DBI::dbExecute(to_conn, "
--------------------
-- kccd
--------------------
TRUNCATE TABLE stg_reference.pop_geo_kccd
INSERT INTO stg_reference.pop_geo_kccd
(etl_batch_ID, 
geo_type, 
geo_scope, 
geo_year, 
census_year, 
year, 
r_type, 
geo_id, 
age, 
age_5, 
age_11, 
age_20, 
age_100, 
gender, 
pop, 
race_wht, 
race_blk, 
race_aian, 
race_as, 
race_nhpi, 
race_hisp, 
rcode, 
r1r3, 
r2r4, 
fips_co, 
raw_agestr, 
raw_gender, 
raw_racemars, 
raw_hispanic)
SELECT 
b.etl_batch_id,
'kccd',
'kc',
x.geo_year,
x.census_year,
b.year,
b.r_type,
x.target_id,
b.age, 
b.age_5, 
b.age_11, 
b.age_20, 
b.age_100, 
b.gender, 
SUM(b.pop * x.proportion), 
b.race_wht, 
b.race_blk, 
b.race_aian, 
b.race_as, 
b.race_nhpi, 
b.race_hisp, 
b.rcode, 
b.r1r3, 
b.r2r4, 
b.fips_co, 
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic
FROM stg_reference.pop_geo_blk b 
INNER JOIN stg_reference.pop_geoxwalks x ON b.census_year = x.census_year AND b.geo_year = x.geo_year AND b.geo_id = x.geo_id
WHERE x.target_type = 'kccd'
GROUP BY 
b.etl_batch_id,
x.geo_year,
x.census_year,
b.year,
b.r_type,
x.target_id,
b.age, 
b.age_5, 
b.age_11, 
b.age_20, 
b.age_100, 
b.gender, 
b.race_wht, 
b.race_blk, 
b.race_aian, 
b.race_as, 
b.race_nhpi, 
b.race_hisp, 
b.rcode, 
b.r1r3, 
b.r2r4, 
b.fips_co, 
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic")
  
  message(paste0("[", strptime(Sys.time(), format = "%Y-%m-%d %H:%M", tz = "GMT"), "] ...loading [pop_geo_PUMA]..."))
  DBI::dbExecute(to_conn, "
--------------------
-- PUMA
--------------------
TRUNCATE TABLE stg_reference.pop_geo_PUMA
INSERT INTO stg_reference.pop_geo_PUMA
(etl_batch_ID, 
geo_type, 
geo_scope, 
geo_year, 
census_year, 
year, 
r_type, 
geo_id, 
age, 
age_5, 
age_11, 
age_20, 
age_100, 
gender, 
pop, 
race_wht, 
race_blk, 
race_aian, 
race_as, 
race_nhpi, 
race_hisp, 
rcode, 
r1r3, 
r2r4, 
fips_co, 
raw_agestr, 
raw_gender, 
raw_racemars, 
raw_hispanic)
SELECT 
b.etl_batch_id,
'PUMA',
'kc',
x.geo_year,
x.census_year,
b.year,
b.r_type,
x.target_id,
b.age, 
b.age_5, 
b.age_11, 
b.age_20, 
b.age_100, 
b.gender, 
SUM(b.pop * x.proportion), 
b.race_wht, 
b.race_blk, 
b.race_aian, 
b.race_as, 
b.race_nhpi, 
b.race_hisp, 
b.rcode, 
b.r1r3, 
b.r2r4, 
b.fips_co, 
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic
FROM stg_reference.pop_geo_blk b 
INNER JOIN stg_reference.pop_geoxwalks x ON b.census_year = x.census_year AND b.geo_year = x.geo_year AND b.geo_id = x.geo_id
WHERE x.target_type = 'PUMA'
GROUP BY 
b.etl_batch_id,
x.geo_year,
x.census_year,
b.year,
b.r_type,
x.target_id,
b.age, 
b.age_5, 
b.age_11, 
b.age_20, 
b.age_100, 
b.gender, 
b.race_wht, 
b.race_blk, 
b.race_aian, 
b.race_as, 
b.race_nhpi, 
b.race_hisp, 
b.rcode, 
b.r1r3, 
b.r2r4, 
b.fips_co, 
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic")
  
  message(paste0("[", strptime(Sys.time(), format = "%Y-%m-%d %H:%M", tz = "GMT"), "] ...loading [pop_geo_tribal]..."))
  DBI::dbExecute(to_conn, "
--------------------
-- tribal
--------------------
TRUNCATE TABLE stg_reference.pop_geo_tribal
INSERT INTO stg_reference.pop_geo_tribal
(etl_batch_ID, 
geo_type, 
geo_scope, 
geo_year, 
census_year, 
year, 
r_type, 
geo_id, 
age, 
age_5, 
age_11, 
age_20, 
age_100, 
gender, 
pop, 
race_wht, 
race_blk, 
race_aian, 
race_as, 
race_nhpi, 
race_hisp, 
rcode, 
r1r3, 
r2r4, 
fips_co, 
raw_agestr, 
raw_gender, 
raw_racemars, 
raw_hispanic)
SELECT 
b.etl_batch_id,
'tribal',
'kc',
x.geo_year,
x.census_year,
b.year,
b.r_type,
x.target_id,
b.age, 
b.age_5, 
b.age_11, 
b.age_20, 
b.age_100, 
b.gender, 
SUM(b.pop * x.proportion), 
b.race_wht, 
b.race_blk, 
b.race_aian, 
b.race_as, 
b.race_nhpi, 
b.race_hisp, 
b.rcode, 
b.r1r3, 
b.r2r4, 
b.fips_co, 
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic
FROM stg_reference.pop_geo_blk b 
INNER JOIN stg_reference.pop_geoxwalks x ON b.census_year = x.census_year AND b.geo_year = x.geo_year AND b.geo_id = x.geo_id
WHERE x.target_type = 'tribal'
GROUP BY 
b.etl_batch_id,
x.geo_year,
x.census_year,
b.year,
b.r_type,
x.target_id,
b.age, 
b.age_5, 
b.age_11, 
b.age_20, 
b.age_100, 
b.gender, 
b.race_wht, 
b.race_blk, 
b.race_aian, 
b.race_as, 
b.race_nhpi, 
b.race_hisp, 
b.rcode, 
b.r1r3, 
b.r2r4, 
b.fips_co, 
b.raw_agestr, 
b.raw_gender, 
b.raw_racemars, 
b.raw_hispanic")
}





