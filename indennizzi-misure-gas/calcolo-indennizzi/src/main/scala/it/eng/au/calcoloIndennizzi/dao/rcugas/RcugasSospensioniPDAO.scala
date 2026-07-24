package it.eng.au.calcoloIndennizzi.dao.rcugas

import it.eng.au.calcoloIndennizzi.dao.DAO
import it.eng.au.calcoloIndennizzi.schema.rcugas.RcugasSospensioniPSchema
import it.eng.au.calcoloIndennizzi.utility.Properties
import it.eng.au.calcoloIndennizzi.utility.constants.Constants.{TIMESTAMP_MS_FORMAT, YEAR_MONTH_FORMAT}
import it.eng.au.indennizziMisureGasCommon.utility.dataframe.DataFrameUtils.isNotNullNorEmpty
import it.eng.au.indennizziMisureGasCommon.utility.date.DateUtility.intersectDfWithYearMonth
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/** Tabella su rcugas contenente le informazioni sui PdR sospesi (che vanno rimossi dal calcolo). */
class RcugasSospensioniPDAO extends DAO {
  val parquetPath: String = Properties.getRcugasSospensioniPath
  val columns: List[String] = RcugasSospensioniPSchema.getValues

  /** Ottiene il dataframe dei PdR sospesi, assieme all'`n_id_fornitura`. */
  override def get: DataFrame = {
    intersectDfWithYearMonth(readParquet,
      RcugasSospensioniPSchema.d_data_inizio_sosp.toString,
      RcugasSospensioniPSchema.d_data_revoca_sosp.toString,
      TIMESTAMP_MS_FORMAT,
      Properties.getYearMonth,
      YEAR_MONTH_FORMAT)
      .where(isNotNullNorEmpty(col(RcugasSospensioniPSchema.n_id_pdr)))
      .select(
        RcugasSospensioniPSchema.n_id_pdr,
        RcugasSospensioniPSchema.n_id_fornitura,
        RcugasSospensioniPSchema.d_data_inizio_sosp,
        RcugasSospensioniPSchema.d_data_revoca_sosp)
      .distinct()
  }
}
