package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasFornituraPSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.joda.time.format.DateTimeFormat

/** Contiene le informazioni sulla fornitura (data inizio e data fine) */
class RcuGasFornituraPDAO extends Dao{
  override val parquetPath: String = Environment.getRcugasFornituraPath
  override val columns: List[String] = List(
    RcuGasFornituraPSchema.n_id_fornitura,
    RcuGasFornituraPSchema.d_data_inizio,
    RcuGasFornituraPSchema.d_data_fine
  )

  def get(): DataFrame = {
    val computationStartDate = lit(DateTimeFormat.forPattern("yyyyMM")
      .parseDateTime(Environment.getPeriodStartDate)
      .dayOfMonth().withMinimumValue().toString("yyyy-MM-dd")
    ).cast(DateType)
    val computationEndDate = lit(DateTimeFormat.forPattern("yyyyMM")
      .parseDateTime(Environment.getPeriodEndDate)
      .dayOfMonth().withMaximumValue().toString("yyyy-MM-dd")
    ).cast(DateType)

    this.readParquet
      .withColumn(RcuGasFornituraPSchema.d_data_inizio, from_unixtime(unix_timestamp(col(RcuGasFornituraPSchema.d_data_inizio), "yyyy-MM-dd HH:mm:ss.S")).cast(DateType))
      .withColumn(RcuGasFornituraPSchema.d_data_fine, from_unixtime(unix_timestamp(col(RcuGasFornituraPSchema.d_data_fine), "yyyy-MM-dd HH:mm:ss.S")).cast(DateType))
      .withColumn(RcuGasFornituraPSchema.d_data_inizio, coalesce(col(RcuGasFornituraPSchema.d_data_inizio), from_unixtime(unix_timestamp(lit("1492-12-31"), "yyyy-MM-dd")).cast(DateType)))
      .withColumn(RcuGasFornituraPSchema.d_data_fine, coalesce(col(RcuGasFornituraPSchema.d_data_fine), from_unixtime(unix_timestamp(lit("2999-12-31"), "yyyy-MM-dd")).cast(DateType)))
      .where(col(RcuGasFornituraPSchema.d_data_inizio) <= computationEndDate)
      .where(col(RcuGasFornituraPSchema.d_data_fine) >= computationStartDate)
  }

}
