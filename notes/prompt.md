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

