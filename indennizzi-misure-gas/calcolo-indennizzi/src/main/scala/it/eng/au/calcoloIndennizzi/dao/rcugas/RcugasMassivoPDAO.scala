package it.eng.au.calcoloIndennizzi.dao.rcugas

import it.eng.au.calcoloIndennizzi.controller.PdrGController.dataAttivazione
import it.eng.au.calcoloIndennizzi.dao.DAO
import it.eng.au.calcoloIndennizzi.dao.DAO.{daysCount, daysCountPerPdr, distinctPivaCount}
import it.eng.au.calcoloIndennizzi.schema.rcugas.RcugasMassivoPSchema
import it.eng.au.calcoloIndennizzi.utility.Properties
import it.eng.au.calcoloIndennizzi.utility.constants.Constants.{TIMESTAMP_MS_FORMAT, YEAR_MONTH_FORMAT}
import it.eng.au.indennizziMisureGasCommon.utility.dataframe.DataFrameUtils.isNotNullNorEmpty
import it.eng.au.indennizziMisureGasCommon.utility.date.DateUtility.intersectDfWithYearMonth
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

import scala.collection.mutable

/** Tabella in rcugas contenente le principali informazioni sui PdR, tra cui la fornitura (con le date di inizio e fine fornitura). */
class RcugasMassivoPDAO extends DAO {
  val parquetPath: String = Properties.getRcugasMassivoPath
  val columns: List[String] = RcugasMassivoPSchema.getValues

  /** Ottiene il dataframe che contiene informazioni base sui PdR e la fornitura. In particolare,
   *  - per un certo PdR non vi possono essere due piva_udd distinte nello stesso mese. Per controllare ciò, utilizziamo la colonna [[distinctPivaCount]];
   *  - per un certo PdR non vi possono essere dei "buchi" all'interno del mese. Per controllare ciò, utilizziamo la colonna [[daysCountPerPdr]]. */
  override def get: DataFrame = {
    val pdrWindow = Window.partitionBy(col(RcugasMassivoPSchema.t_codice_pdr))
    val daysInMonth = Properties.getDaysInMonth.toInt

    val rcugasMassivo = readParquet
      .withColumn(dataAttivazione, from_unixtime(unix_timestamp(col(RcugasMassivoPSchema.d_data_inizio_for), TIMESTAMP_MS_FORMAT)).cast(DateType))

    intersectDfWithYearMonth(rcugasMassivo,
      RcugasMassivoPSchema.d_data_inizio_for.toString,
      RcugasMassivoPSchema.data_fine_for.toString,
      TIMESTAMP_MS_FORMAT,
      Properties.getYearMonth,
      YEAR_MONTH_FORMAT)
      .where(isNotNullNorEmpty(col(RcugasMassivoPSchema.t_codice_pdr)))
      .where(isNotNullNorEmpty(col(RcugasMassivoPSchema.piva_udd)))
      .withColumn(daysCount, datediff(col(RcugasMassivoPSchema.data_fine_for), col(RcugasMassivoPSchema.d_data_inizio_for)) + 1)
      .withColumn(daysCountPerPdr, sum(col(daysCount)).over(pdrWindow))
      .withColumn(distinctPivaCount, size(collect_set(RcugasMassivoPSchema.piva_udd).over(pdrWindow)))
      .where(col(daysCountPerPdr) === daysInMonth)
      .where(col(distinctPivaCount) === 1)
      .select(
        col(RcugasMassivoPSchema.n_id_pdr),
        col(RcugasMassivoPSchema.t_codice_pdr),
        col(RcugasMassivoPSchema.n_id_fornitura),
        col(RcugasMassivoPSchema.piva_udd),
        col(dataAttivazione),
        col(RcugasMassivoPSchema.d_data_inizio_for),
        col(RcugasMassivoPSchema.data_fine_for))
  }

  def concatCollectSetUDF: UserDefinedFunction = udf((list: mutable.WrappedArray[String], sep: String) => list.mkString(sep))
}