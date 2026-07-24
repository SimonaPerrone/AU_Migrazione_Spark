package it.eng.au.calcoloIndennizzi.dao.rcugas

import it.eng.au.calcoloIndennizzi.dao.DAO
import it.eng.au.calcoloIndennizzi.dao.DAO.{daysCount, daysCountPerPdr}
import it.eng.au.calcoloIndennizzi.schema.rcugas.RcugasVarMisuratoreSchema
import it.eng.au.calcoloIndennizzi.utility.Properties
import it.eng.au.calcoloIndennizzi.utility.constants.Constants.{TIMESTAMP_MS_FORMAT, YEAR_MONTH_FORMAT, admissibleClasseMisuratoreList}
import it.eng.au.indennizziMisureGasCommon.utility.dataframe.DataFrameUtils.isNotNullNorEmpty
import it.eng.au.indennizziMisureGasCommon.utility.date.DateUtility.intersectDfWithYearMonth
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

import scala.collection.mutable

/** Tabella su rcugas contenente le informazioni sulla classe misuratore. */
class RcugasVarMisuratoreDAO extends DAO {
  val parquetPath: String = Properties.getRcugasVarMisuratorePath
  val columns: List[String] = RcugasVarMisuratoreSchema.getValues

  /** Ottiene il dataframe contenente le informazioni sulla classe misuratore.
   * Affinché un PdR sia nel perimetro, la sua classe misuratore deve rientrare tra quelle in [[admissibleClasseMisuratoreList]] per tutto il mese. */
  override def get: DataFrame = {
    val pdrWindow = Window.partitionBy(col(RcugasVarMisuratoreSchema.n_id_pdr))
    val daysInMonth = Properties.getDaysInMonth.toInt

    intersectDfWithYearMonth(readParquet,
      RcugasVarMisuratoreSchema.d_data_inizio.toString,
      RcugasVarMisuratoreSchema.d_data_fine.toString,
      TIMESTAMP_MS_FORMAT,
      Properties.getYearMonth,
      YEAR_MONTH_FORMAT)
      .where(isNotNullNorEmpty(col(RcugasVarMisuratoreSchema.n_id_pdr)))
      .where(col(RcugasVarMisuratoreSchema.t_classe_misuratore).isin(admissibleClasseMisuratoreList: _*))
      .withColumn(daysCount, datediff(col(RcugasVarMisuratoreSchema.d_data_fine), col(RcugasVarMisuratoreSchema.d_data_inizio)) + 1)
      .withColumn(daysCountPerPdr, sum(col(daysCount)).over(pdrWindow))
      .where(col(daysCountPerPdr) === daysInMonth)

      .groupBy(col(RcugasVarMisuratoreSchema.n_id_pdr))
      .agg(collect_set(col(RcugasVarMisuratoreSchema.t_classe_misuratore)).alias(RcugasVarMisuratoreSchema.t_classe_misuratore))
      .withColumn(RcugasVarMisuratoreSchema.t_classe_misuratore, concatCollectSetUDF(col(RcugasVarMisuratoreSchema.t_classe_misuratore), lit(",")))
  }

  def concatCollectSetUDF: UserDefinedFunction = udf((list: mutable.WrappedArray[String], sep: String) => list.mkString(sep))
}
