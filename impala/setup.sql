-- Impala reads the SAME tables as Hive through the shared metastore DB (PLAN.md §3.2, "As built").
-- Event polling is off (Apache HMS 3.1.3), so pick up new tables/partitions explicitly.
--
-- No COMPUTE STATS here: Impala's CDP metastore stores column statistics tagged ENGINE='impala' next to
-- Hive's, and Apache Hive 3.1.3 (which has no ENGINE column) then sees two sets per column and fails
-- every later INSERT ("Unexpected 28 statistics for 14 columns"). Impala plans with the table-level
-- statistics Hive already maintains.
INVALIDATE METADATA;
