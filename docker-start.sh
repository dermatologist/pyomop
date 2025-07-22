
docker run --rm -it --network=pyomop_ohdsi \
  -e CDM_VERSION=5.4 \
  -e PGPASSWORD=ohdsi \
  -e PGUSER=ohdsi \
  -e PGHOST=omopdb \
  -e PGDATABASE=ohdsi \
  -e WEBAPI_URL=http://webapi.pyomop_ohdsi:8080/WebAPI \
  -e SETUP_CDS=true \
  -e SETUP_SYNPUF=true \
  cr.ukdd.de/pub/ohdsi/techstack:v2025.02
