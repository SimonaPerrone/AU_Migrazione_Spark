ALTER TABLE ${hive.db}.ca_final
ADD COLUMNS (
pres_tds BOOLEAN,
massivo_freeze_executionid BIGINT,
freeze_date TIMESTAMP
)