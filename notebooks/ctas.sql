-- Databricks notebook source
create or replace table IDENTIFIER(CONCAT('$tgt_catalog','.', '$tgt_schema', '.', '$tgt_table')) as
select * from IDENTIFIER(CONCAT('$src_catalog','.', '$src_schema', '.', '$src_table'))