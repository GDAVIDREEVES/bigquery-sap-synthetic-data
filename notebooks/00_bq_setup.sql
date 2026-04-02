-- BigQuery: dataset for synthetic ACDOCA output (adjust names and region).
--
-- The PySpark job uses the Spark BigQuery connector with CREATE_IF_NEEDED on first write.
-- Partitioning and clustering are applied by the writer (see acdoca_generator.utils.spark_writer):
--   time partitioning: BUDAT, MONTH
--   clustering: RBUKRS, GJAHR, POPER
--
-- If you pre-create an empty table, its partitioning must be compatible with that layout or
-- omit the table and let the first write create it.

-- Replace MY_PROJECT with your GCP project id.
CREATE SCHEMA IF NOT EXISTS `MY_PROJECT.synthetic_acdoca`
OPTIONS (
  location = 'US',
  description = 'Synthetic SAP ACDOCA-style journal data (generator output)'
);

-- Optional: reserve a table name without columns (not required — first connector write creates the table).
-- CREATE TABLE IF NOT EXISTS `MY_PROJECT.synthetic_acdoca.journal_entries` (
--   _placeholder STRING
-- );
