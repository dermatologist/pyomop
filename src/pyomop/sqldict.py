"""Predefined OMOP SQL snippets from the OHDSI Query Library.

This module exposes a small dictionary of named SQL queries that can be used
for demos, tests, or quick analytics over a CDM instance.

Source: https://github.com/OHDSI/QueryLibrary/tree/master/inst/shinyApps/QueryLibrary/queries
"""

# https://github.com/OHDSI/QueryLibrary/tree/master/inst/shinyApps/QueryLibrary/queries

# Test
TEST = "SELECT * from cohort;"


# PE03: Number of patients grouped by gender
PE03 = """
SELECT
  person.gender_concept_id,
  concept.concept_name    AS gender_name,
  COUNT(person.person_id) AS num_persons
FROM person
  JOIN concept ON person.gender_concept_id = concept.concept_id
GROUP BY person.gender_concept_id, concept.concept_name;
"""
CDMSQL = {"TEST": TEST, "PE03": PE03}
