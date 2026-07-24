package it.eng.au.calcoloIndennizzi.dao.rcugas

import it.eng.au.calcoloIndennizzi.dao.DAO
import it.eng.au.calcoloIndennizzi.dao.DAO.{daysCount, daysCountPerPdr, distinctPivaCount}
import it.eng.au.calcoloIndennizzi.schema.rcugas.RcugasConnessioniDistr2Schema
import it.eng.au.calcoloIndennizzi.utility.Properties
import it.eng.au.calcoloIndennizzi.utility.constants.Constants.{TIMESTAMP_MS_FORMAT, YEAR_MONTH_FORMAT}
import it.eng.au.indennizziMisureGasCommon.utility.dataframe.DataFrameUtils.isNotNullNorEmpty
import it.eng.au.indennizziMisureGasCommon.utility.date.DateUtility.intersectDfWithYearMonth
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Tabella in rcugas contenente le informazioni sul distributore (principalmente piva_distr). */
class RcugasConnessioniDistr2DAO extends DAO {
  val parquetPath: String = Properties.getRcugasConnessioniDistr2Path
  val columns: List[String] = RcugasConnessioniDistr2Schema.getValues

  /** Ottiene il dataframe che contiene informazioni sui PdR e le piva_distr. In particolare,
   *  - per un certo PdR non vi possono essere due piva_distr distinte nello stesso mese. Per controllare ciò, utilizziamo la colonna [[distinctPivaCount]];
   *  - per un certo PdR non vi possono essere dei "buchi" all'interno del mese. Per controllare ciò, utilizziamo la colonna [[daysCountPerPdr]]. */
  override def get: DataFrame = {
    val pdrWindow = Window.partitionBy(col(RcugasConnessioniDistr2Schema.t_codice_pdr))
    val daysInMonth = Properties.getDaysInMonth.toInt

    intersectDfWithYearMonth(readParquet,
      RcugasConnessioniDistr2Schema.d_data_inizio_conn.toString,
      RcugasConnessioniDistr2Schema.d_data_fine_conn.toString,
      TIMESTAMP_MS_FORMAT,
      Properties.getYearMonth,
      YEAR_MONTH_FORMAT)
      .where(isNotNullNorEmpty(col(RcugasConnessioniDistr2Schema.t_codice_pdr)))
      .where(isNotNullNorEmpty(col(RcugasConnessioniDistr2Schema.t_piva_distr)))
      .withColumn(daysCount, datediff(col(RcugasConnessioniDistr2Schema.d_data_fine_conn), col(RcugasConnessioniDistr2Schema.d_data_inizio_conn)) + 1)
      .withColumn(daysCountPerPdr, sum(col(daysCount)).over(pdrWindow))
      .withColumn(distinctPivaCount, size(collect_set(RcugasConnessioniDistr2Schema.t_piva_distr).over(pdrWindow)))
      .where(col(daysCountPerPdr) === daysInMonth)
      .where(col(distinctPivaCount) === 1)
      .select(
        RcugasConnessioniDistr2Schema.t_codice_pdr,
        RcugasConnessioniDistr2Schema.t_piva_distr
      )
      .distinct()
  }
}
