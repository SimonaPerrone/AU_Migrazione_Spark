-- Tabella di log pubblicazione file ERP aggregato
DROP TABLE IF EXISTS ${hive.table.erp.erp_aggregato_pub};
CREATE TABLE ${hive.table.erp.erp_aggregato_pub} (
    ANNO INT,
    MESE INT,
    TIPO_PUBB STRING,          -- AC2 (Terna) | AC4 (Distributori)
    PIVA_DISTR STRING,
    NOME_FILE STRING,
    TIPO_FILE STRING,          -- XML | CSV | ZIP
    PATH STRING,               -- Percorso completo del file pubblicato
    EXECUTIONID_IN STRING,     -- executionid della run ERP di input (sorgente)
    EXECUTIONID STRING         -- executionid della pubblicazione
)
STORED AS PARQUET;

