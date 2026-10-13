-- Lets Impala's metastore (CDP Hive 3.1.3000, which speaks parts of the Hive 4 API) share the metastore
-- DB created by Apache Hive 3.1.3. Additive only; the Apache metastore ignores these columns.
-- Only NOT NULL columns need to exist up front (with the defaults the Hive 4 schema uses); the CDP
-- metastore's DataNucleus (datanucleus.schema.autoCreateAll) adds the remaining nullable columns and
-- the new tables (DATACONNECTORS, DC_PRIVS, ...) itself. Idempotent.
ALTER TABLE "DBS"            ADD COLUMN IF NOT EXISTS "TYPE"   varchar(32)  NOT NULL DEFAULT 'NATIVE';
ALTER TABLE "TAB_COL_STATS"  ADD COLUMN IF NOT EXISTS "ENGINE" varchar(128) NOT NULL DEFAULT 'hive';
ALTER TABLE "PART_COL_STATS" ADD COLUMN IF NOT EXISTS "ENGINE" varchar(128) NOT NULL DEFAULT 'hive';
-- ACID write ids (primitive long in the CDP model => NOT NULL); 0 = "not transactional", as in Hive 4
ALTER TABLE "TBLS"           ADD COLUMN IF NOT EXISTS "WRITE_ID" bigint NOT NULL DEFAULT 0;
ALTER TABLE "PARTITIONS"     ADD COLUMN IF NOT EXISTS "WRITE_ID" bigint NOT NULL DEFAULT 0;
ALTER TABLE "TAB_COL_STATS"  ADD COLUMN IF NOT EXISTS "HISTOGRAM" bytea;
ALTER TABLE "PART_COL_STATS" ADD COLUMN IF NOT EXISTS "HISTOGRAM" bytea;
ALTER TABLE "METASTORE_DB_PROPERTIES" ADD COLUMN IF NOT EXISTS "PROPERTYCONTENT" bytea;
-- Never keep non-Hive column statistics: Apache Hive 3.1.3 can't tell engines apart (see impala/setup.sql)
DELETE FROM "TAB_COL_STATS"  WHERE "ENGINE" <> 'hive';
DELETE FROM "PART_COL_STATS" WHERE "ENGINE" <> 'hive';
