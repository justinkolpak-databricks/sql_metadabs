-- Databricks notebook source
-- Creates streaming table that ingests json files
CREATE OR REFRESH STREAMING TABLE IDENTIFIER('${tgt_catalog}' || '.' || '${tgt_schema}' || '.' || '${tgt_table}')
COMMENT '${table_comment}'
TBLPROPERTIES (${table_properties})
AS SELECT ${select_list}
FROM STREAM read_files(
  '${src_path}',
  format => 'json'
);