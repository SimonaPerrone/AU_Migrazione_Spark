package it.eng.au.calcoloIndennizzi.dao.rcugas

import it.eng.au.calcoloIndennizzi.dao.DAO
import it.eng.au.calcoloIndennizzi.dao.DAO.{daysCount, daysCountPerPdr}
import it.eng.au.calcoloIndennizzi.schema.rcugas.RcugasVarTrattamentoPSchema
import it.eng.au.calcoloIndennizzi.utility.Properties
import it.eng.au.calcoloIndennizzi.utility.constants.Constants.{TIMESTAMP_MS_FORMAT, TRATTAMENTO_G, YEAR_MONTH_FORMAT}
import it.eng.au.indennizziMisureGasCommon.utility.dataframe.DataFrameUtils.isNotNullNorEmpty
import it.eng.au.indennizziMisureGasCommon.utility.date.DateUtility.intersectDfWithYearMonth
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, datediff, sum}

/** Tabella su rcugas contenente le informazioni sul trattamento. */
class RcugasVarTrattamentoPDAO extends DAO {
  val parquetPath: String = Properties.getRcugasVarTrattamentoPath
  val columns: List[String] = RcugasVarTrattamentoPSchema.getValues

  /** Ottiene il dataframe contenente le informazioni sul trattamento.
   * Affinché un PdR sia nel perimetro, il suo trattamento deve essere "G" in tutto il mese. */
  override def get: DataFrame = {
    val pdrWindow = Window.partitionBy(col(RcugasVarTrattamentoPSchema.n_id_pdr))
    val daysInMonth = Properties.getDaysInMonth.toInt

    intersectDfWithYearMonth(readParquet,
      RcugasVarTrattamentoPSchema.d_data_inizio.toString,
      RcugasVarTrattamentoPSchema.d_data_fine.toString,
      TIMESTAMP_MS_FORMAT,
      Properties.getYearMonth,
      YEAR_MONTH_FORMAT
    )
      .where(isNotNullNorEmpty(col(RcugasVarTrattamentoPSchema.n_id_pdr)))
      .where(col(RcugasVarTrattamentoPSchema.t_trattamento_settlement) === TRATTAMENTO_G)
      .withColumn(daysCount, datediff(col(RcugasVarTrattamentoPSchema.d_data_fine), col(RcugasVarTrattamentoPSchema.d_data_inizio)) + 1)
      .withColumn(daysCountPerPdr, sum(col(daysCount)).over(pdrWindow))
      .where(col(daysCountPerPdr) === daysInMonth)
      .select(RcugasVarTrattamentoPSchema.n_id_pdr)
      .distinct()
  }
}
