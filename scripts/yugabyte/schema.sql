-- schema.sql
CREATE DATABASE fanoutdb;

\c fanoutdb

DROP TABLE IF EXISTS yb_counters;

CREATE TABLE yb_counters (
    k BIGINT PRIMARY KEY,
    v BIGINT NOT NULL DEFAULT 0
) SPLIT INTO 9 TABLETS;
