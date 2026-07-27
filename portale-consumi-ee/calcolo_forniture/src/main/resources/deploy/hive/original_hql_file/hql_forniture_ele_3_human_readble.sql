-- Define temporary macros
CREATE TEMPORARY MACRO isNumber(s STRING) CAST(s AS DOUBLE) IS NOT NULL;
CREATE TEMPORARY MACRO isNumeric(s STRING) s NOT RLIKE '[^0-9]';

-- Drop the existing table if it exists
DROP TABLE mongodbs.RCU_POD_DISTR;

-- Create the `RCU_POD_DISTR` table
CREATE TABLE mongodbs.RCU_POD_DISTR STORED AS PARQUET AS
SELECT 
    RCU_POD_DISTR.n_id_pod,
    RCU_AZIENDA.T_rag_soc 
FROM RCU.RCU_POD_DISTR_p AS RCU_POD_DISTR
JOIN (
    SELECT 
        CAST(n_id_azienda AS STRING) AS n_id_azienda,
        t_rag_soc AS T_rag_soc,
        t_piva
    FROM rcu.rcu_azienda_p 
    WHERE isNumeric(n_id_azienda) = TRUE
) AS RCU_AZIENDA
ON RCU_AZIENDA.n_id_azienda = NVL(RCU_POD_DISTR.n_id_distr, '');
