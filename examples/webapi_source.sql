INSERT INTO webapi.source (source_id, source_name, source_key, source_connection, source_dialect)
SELECT nextval('webapi.source_sequence'), 'Demo CDM', 'DEMO_CDM', 'jdbc:postgresql://localhost:5432/postgres?user=postgres&password=mypass', 'postgresql';

INSERT INTO webapi.source_daimon (source_daimon_id, source_id, daimon_type, table_qualifier, priority)
SELECT nextval('webapi.source_daimon_sequence'), source_id, 0, 'demo_cdm', 0
FROM webapi.source
WHERE source_key = 'DEMO_CDM'
;

INSERT INTO webapi.source_daimon (source_daimon_id, source_id, daimon_type, table_qualifier, priority)
SELECT nextval('webapi.source_daimon_sequence'), source_id, 1, 'demo_cdm', 1
FROM webapi.source
WHERE source_key = 'DEMO_CDM'
;

INSERT INTO webapi.source_daimon (source_daimon_id, source_id, daimon_type, table_qualifier, priority)
SELECT nextval('webapi.source_daimon_sequence'), source_id, 2, 'demo_cdm_results', 1
FROM webapi.source
WHERE source_key = 'DEMO_CDM'
;

INSERT INTO webapi.source_daimon (source_daimon_id, source_id, daimon_type, table_qualifier, priority)
SELECT nextval('webapi.source_daimon_sequence'), source_id, 5, 'demo_temp', 0
FROM webapi.source
WHERE source_key = 'DEMO_CDM'
;

INSERT INTO webapi.source (source_id, source_name, source_key, source_connection, source_dialect)
SELECT nextval('webapi.source_sequence'), 'My CDM', 'MY_CDM', 'jdbc:postgresql://localhost:5432/postgres?user=postgres&password=mypass', 'postgresql';

INSERT INTO webapi.source_daimon (source_daimon_id, source_id, daimon_type, table_qualifier, priority)
SELECT nextval('webapi.source_daimon_sequence'), source_id, 0, 'public', 0
FROM webapi.source
WHERE source_key = 'MY_CDM'
;

INSERT INTO webapi.source_daimon (source_daimon_id, source_id, daimon_type, table_qualifier, priority)
SELECT nextval('webapi.source_daimon_sequence'), source_id, 1, 'public', 1
FROM webapi.source
WHERE source_key = 'MY_CDM'
;

INSERT INTO webapi.source_daimon (source_daimon_id, source_id, daimon_type, table_qualifier, priority)
SELECT nextval('webapi.source_daimon_sequence'), source_id, 2, 'demo_cdm_results', 1
FROM webapi.source
WHERE source_key = 'MY_CDM'
;

INSERT INTO webapi.source_daimon (source_daimon_id, source_id, daimon_type, table_qualifier, priority)
SELECT nextval('webapi.source_daimon_sequence'), source_id, 5, 'demo_temp', 0
FROM webapi.source
WHERE source_key = 'MY_CDM'
;
