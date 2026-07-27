DROP TABLE IF EXISTS ${hive.table.erp.erp_validated_mis_no};
CREATE EXTERNAL TABLE ${hive.table.erp.erp_validated_mis_no} (
     pod             STRING,
     anno            STRING,
     mese            STRING,
     data_misura     STRING,
     tipo_dato_s     STRING,
     tipo_dato_e     STRING,
     time_stamp      STRING,
     eam             STRING,
     eaf1            STRING,
     eaf2            STRING,
     eaf3            STRING,
     tipo_flusso     STRING,
     nomefile        STRING,
     coeff_perdita   STRING,
     k               STRING,
     trattamento     STRING
 )
 PARTITIONED BY (
     executionid     STRING
 )
STORED AS PARQUET
LOCATION '/user/eng_test/ERP/dev/output/erp_validated_mis_no/';

