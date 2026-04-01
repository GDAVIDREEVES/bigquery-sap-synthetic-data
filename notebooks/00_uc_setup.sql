-- Databricks notebook source
-- Run once to create the default target schema (adjust names as needed).

-- COMMAND ----------
CREATE CATALOG IF NOT EXISTS synthetic;

-- COMMAND ----------
CREATE SCHEMA IF NOT EXISTS synthetic.acdoca;

-- COMMAND ----------
-- Optional: grant access to a group/service principal (replace with your principal).
-- GRANT USE CATALOG ON CATALOG synthetic TO `finance-analytics`;
-- GRANT USE SCHEMA ON SCHEMA synthetic.acdoca TO `finance-analytics`;
-- GRANT CREATE, MODIFY ON SCHEMA synthetic.acdoca TO `finance-analytics`;

