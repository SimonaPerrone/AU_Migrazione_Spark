ALTER TABLE ${hive.db}.ca_pre_final
ADD COLUMNS (
trattamento_forced STRING,
massivo_freeze_executionid BIGINT,
freeze_date TIMESTAMP
)