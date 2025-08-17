# Prompts

write code to load a single csv file to multiple tables in a database using sql alchemy and an external mapping file.
All rows in the csv have an patient_id column
The sample csv file is referenced.

##

Write a function fix_person_id that updates all tables to replace the person_id foreign key with the person_id instead of person_source_value that it currently uses.

##

In the person table, replace 0 and null values in year_of_birth, month_of_birth and day_of_birth with the values from birth_datetime.

##

Write a function vocabulary_lookup that populates columns ending with _concept_id with concept_id values from the vocabulary table mapped based on the corresponding _source_value columns in the OMOP CDM.

##

Based on the concept key in mapping.example.json, implement the concept table lookup function.
If source is a comma separated string value, perform the lookup for the first element only.
Find the source field value in concept.concept_code
Update the target field to concept.concept_id

##

Add a function for updating gender_concept_id column in person table according to static values as below.
The standard concept IDs for person gender are 8507 for male and 8532 for female and 0 for unknown.

##

Update the README to include support for FHIR to OMOP mapping.
The user can download the sample FHIR bulk export datasets from [here](https://github.com/smart-on-fhir/sample-bulk-fhir-datasets).
Remind to delete any non FHIR data such as log.ndjson.
Remind to download vocabulary files.
The command is of the form  pyomop --create --vocab ~/Downloads/omop-vocab/ --input ~/Downloads/fhir/
This will create a OMOP CDM using SQLite, load vocabulary files, and import FHIR data.

##

Alter all person_id foreign keys in the tables other than person to type TEXT before import.
Convert the type back to default after step 2: : Normalize person_id FKs using person.person_id

##

REPLY
I'll edit the loader to add a safe, schema-agnostic workaround: add a temporary person_id_text column to non-person tables before inserts, route non-numeric person IDs into it, normalize in step 2 using this column, and drop it right after. This avoids fragile FK/type changes and works across SQLite/Postgres.

##

Set session_replication_role to replica before load and back to origin after load.

##

You have edited the loader to add a safe, schema-agnostic workaround: add a temporary person_id_text column to non-person tables before inserts, route non-numeric person IDs into it,
Add a placeholder patient_id to avoid vioalating non_null constraint
normalize patient_id in step 2 using this patient_id_text column, and drop it right after. This avoids fragile FK/type changes and works across SQLite/Postgres.

###
[SQL: INSERT INTO device_exposure (person_id, device_concept_id, device_exposure_start_date, device_exposure_start_datetime, device_exposure_end_date, device_exposure_end_datetime, device_type_concept_id, unique_device_id, production_id, device_source_value, unit_concept_id, person_id_text) VALUES ($1::INTEGER, $2::INTEGER, $3::DATE, $4::TIMESTAMP WITHOUT TIME ZONE, $5::DATE, $6::TIMESTAMP WITHOUT TIME ZONE, $7::INTEGER, $8::VARCHAR, $9::VARCHAR, $10::VARCHAR, $11::INTEGER, $12::VARCHAR)]
[parameters: [(0, 0, datetime.date(1958, 7, 12), datetime.datetime(1958, 7, 13, 3, 58, 16), datetime.date(1983, 7, 27), datetime.datetime(1983, 7, 28, 3, 58, 16), 0, "[{'deviceIdentifier': '96702913868313', 'carrierHR", 96702913868313.0, '337414009', 0, '79a66c97-6131-3213-f3c9-4606946ab056'), (0, 0, datetime.date(2006, 4, 22), datetime.datetime(2006, 4, 22, 18, 37, 35), datetime.date(2031, 5, 7), datetime.datetime(2031, 5, 7, 18, 37, 35), 0, "[{'deviceIdentifier': '49280888557202', 'carrierHR", 49280888557202.0, '228869008', 0, '6a4160eb-a793-2f86-2302-378626f46cce'), (0, 0, datetime.date(1972, 4, 8), datetime.datetime(1972, 4, 9, 3, 58, 16), datetime.date(1997, 4, 23), datetime.datetime(1997, 4, 24, 3, 58, 16), 0, "[{'deviceIdentifier': '38227843420982', 'carrierHR", 38227843420982.0, '337414009', 0, '129c6ac7-8d06-89de-ad63-0204a93e76c3'), (0, 0, datetime.date(2006, 4, 22), datetime.datetime(2006, 4, 22, 18, 37, 35), datetime.date(2031, 5, 7), datetime.datetime(2031, 5, 7, 18, 37, 35), 0, "[{'deviceIdentifier': '90766908688698', 'carrierHR", 90766908688698.0, '705417005', 0, '6a4160eb-a793-2f86-2302-378626f46cce'), (0, 0, datetime.date(1981, 5, 23), datetime.datetime(1981, 5, 24, 3, 58, 16), datetime.date(2006, 6, 7), datetime.datetime(2006, 6, 8, 3, 58, 16), 0, "[{'deviceIdentifier': '60396823922872', 'carrierHR", 60396823922872.0, '337414009', 0, 'a5cb8ce9-cec6-6b23-0990-cbaf753578a4'), (0, 0, datetime.date(2022, 10, 20), datetime.datetime(2022, 10, 20, 22, 17, 37), datetime.date(2047, 11, 4), datetime.datetime(2047, 11, 4, 22, 17, 37), 0, "[{'deviceIdentifier': '23290103271114', 'carrierHR", 23290103271114.0, '701077002', 0, 'a4a401d1-a46a-eb4a-8a38-760d5d79d6ec'), (0, 0, datetime.date(1970, 11, 16), datetime.datetime(1970, 11, 16, 16, 43, 52), datetime.date(1995, 12, 1), datetime.datetime(1995, 12, 1, 16, 43, 52), 0, "[{'deviceIdentifier': '63726553703576', 'carrierHR", 63726553703576.0, '363753007', 0, '3af3708d-41f1-cd80-f3dd-ec5ac76072bf'), (0, 0, datetime.date(2022, 10, 21), datetime.datetime(2022, 10, 21, 5, 26, 37), datetime.date(2047, 11, 5), datetime.datetime(2047, 11, 5, 5, 26, 37), 0, "[{'deviceIdentifier': '82416332588326', 'carrierHR", 82416332588326.0, '702172008', 0, 'a4a401d1-a46a-eb4a-8a38-760d5d79d6ec')  ... displaying 10 of 16 total bound parameter sets ...  (0, 0, datetime.date(2020, 5, 1), datetime.datetime(2020, 5, 1, 15, 46, 59), datetime.date(2045, 5, 16), datetime.datetime(2045, 5, 16, 15, 46, 59), 0, "[{'deviceIdentifier': '37990429784417', 'carrierHR", 37990429784417.0, '337414009', 0, '7bc002fa-dc52-17d6-1563-fd8901826f7d'), (0, 0, datetime.date(2022, 10, 21), datetime.datetime(2022, 10, 21, 5, 26, 37), datetime.date(2047, 11, 5), datetime.datetime(2047, 11, 5, 5, 26, 37), 0, "[{'deviceIdentifier': '51104341082973', 'carrierHR", 51104341082973.0, '706180003', 0, 'a4a401d1-a46a-eb4a-8a38-760d5d79d6ec')]]
(Background on this error at: https://sqlalche.me/e/20/dbapi)

##

Some values listed below are not cast as TEXT required by omop schema.
production_id : distinctIdentifier
drug_source_value : vaccineCode.coding.codes, medicationCodeableConcept.coding.codes
Add a generic function to cast all fields to the required type by the OMOP schema before inserting into the database.

##

Improve the function def _coerce_record_to_table_types to remove 0 after the decimal point while coercing FLOAT to string.

###

Add support for schema when postgres database is selected.
Log a warning that schemas are not supported for other databases.

###