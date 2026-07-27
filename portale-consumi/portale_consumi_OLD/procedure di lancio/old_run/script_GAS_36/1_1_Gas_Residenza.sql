DROP TABLE misuregas.RCUGAS_RESIDENZA;

CREATE TABLE misuregas.RCUGAS_RESIDENZA stored as parquet as 
SELECT
      residenza_2.n_id_fornitura,
      residenza_2.t_residenza
   FROM (
      SELECT
         rcugas_residenza.n_id_fornitura,
         MAX(rcugas_residenza.d_aggiornamento) as d_aggiornamento
      FROM RCUGAS.RCUGAS_RESIDENZA_p as rcugas_residenza
      GROUP BY rcugas_residenza.n_id_fornitura
   ) AS residenza_1
   INNER JOIN rcugas.rcugas_residenza_p AS residenza_2
   ON residenza_1.n_id_fornitura = residenza_2.n_id_fornitura and
   nvl(residenza_1.d_aggiornamento,'')=nvl(residenza_2.d_aggiornamento,'');