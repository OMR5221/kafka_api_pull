-- DROP DATABASE IF EXISTS es_dw;
DROP USER IF EXISTS es_dw_owner;

CREATE USER es_dw_owner PASSWORD 'password';

-- Create database:
CREATE DATABASE es_dw;

\c es_dw;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO es_dw_owner;

CREATE SCHEMA es_dw AUTHORIZATION es_dw_owner;

GRANT ALL PRIVILEGES ON SCHEMA es_dw TO es_dw_owner;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA es_dw TO es_dw_owner;

\c es_dw;

DROP TABLE IF EXISTS es_plant_dim;
DROP TABLE IF EXISTS es_tags_dim;

CREATE TABLE es_plant_dim (
    id serial PRIMARY KEY,
    plant_id INT NOT NULL,
    plant_code VARCHAR (10) NOT NULL,
    plant_name VARCHAR(30) NOT NULL,
    status VARCHAR(10) NOT NULL,
    plant_capacity NUMERIC
);

CREATE TABLE es_tags_dim (
    TAG_ID SERIAL PRIMARY KEY,
    TAG_NAME VARCHAR(200) NOT NULL,
    PLANT_ID INT NOT NULL,
    TAG_TYPE VARCHAR(30),
    TAG_DESC VARCHAR(200),
    SERVER_NAME VARCHAR(30),
    PI_METHOD VARCHAR(100)
);

GRANT SELECT ON ALL TABLES IN SCHEMA public TO es_dw_owner;

COPY es_plant_dim
FROM '/docker-entrypoint-initdb.d/es_plant_dim.csv' DELIMITER ',' CSV HEADER;

COPY es_tags_dim
FROM '/docker-entrypoint-initdb.d/es_tags_dim.csv' DELIMITER ',' CSV HEADER;
