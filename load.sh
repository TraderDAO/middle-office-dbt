#!/usr/bin/env bash

PGOPTIONS="--search_path=sbinancedata"                                                                                                
export PGOPTIONS 

psql -d dbtpractice -c "DROP TABLE IF EXISTS portfolio;"   
psql -d dbtpractice -c "CREATE SCHEMA IF NOT EXISTS sbinancedata;"   
psql -d dbtpractice -c "CREATE TABLE dbtpractice.sbinancedata.portfolio (
\"order_id\" bigint,
\"name\" text,
\"side\" text,
\"price\" numeric,
\"cost\" numeric,
\"volume_executed\" numeric,
\"time_created\" bigint
);"

psql -d dbtpractice -c "\copy portfolio FROM './seeds/portfolio.csv' HEADER CSV;"
