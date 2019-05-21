# Dockerfile
FROM postgres:11.1-alpine

COPY *\.csv /docker-entrypoint-initdb.d/
ADD create_load_db.sql /docker-entrypoint-initdb.d
