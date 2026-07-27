package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasSospensioniPSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility.filterDfWithStartEndDate
import org.apache.spark.sql.DataFrame

/** Contiene informazioni sui PdR sospesi */
class RcuGasSospensioniPDAO extends Dao {
  override val parquetPath: String = Environment.getRcugasSospensioniPath
  override val columns: List[String] = List(
    RcuGasSospensioniPSchema.n_id_fornitura,
    RcuGasSospensioniPSchema.n_id_pdr,
    RcuGasSospensioniPSchema.d_data_inizio_sosp,
    RcuGasSospensioniPSchema.d_data_revoca_sosp
  )

  def get(): DataFrame = {
    val df = this.readParquet
      .withColumnRenamed(RcuGasSospensioniPSchema.n_id_fornitura, RcuGasSospensioniPSchema.n_id_fornitura_sosp)

    filterDfWithStartEndDate(df
      , RcuGasSospensioniPSchema.d_data_inizio_sosp.toString
      , RcuGasSospensioniPSchema.d_data_revoca_sosp.toString
      , "yyyy-MM-dd HH:mm:ss.S"
      , Environment.getPeriodStartDate
      , Environment.getPeriodEndDate
      , "yyyyMM"
    )

  }

}
