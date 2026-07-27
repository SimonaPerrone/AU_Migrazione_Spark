package it.eng.au.aggiustamentoGas.dao.agg

import it.eng.au.aggiustamentoGas.model.agg.MonthTreatment
import it.eng.au.aggiustamentoGas.schema.agg.MonthTreatmentSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD

/** Tabella di output contenente il trattamento mensile delle misure */
class MonthTreatmentDAO extends AggDao {
  override val tableName: String = Environment.getMonthTreatmentTable
  override val parquetPath: String = Environment.getMonthTreatmentPath
  override val columns: List[String] = MonthTreatmentSchema.values.map(_.toString).toList


  def writeParquet(monthTreatment: RDD[MonthTreatment], startDate: String, endDate: String): Unit = {
    val monthTreatmentFiltered = monthTreatment.filter(t =>
      t.month >= startDate && t.month <= endDate
    )

    val df = Environment.getSpark.sqlContext.createDataFrame(monthTreatmentFiltered)

    writeParquet(df.coalesce(df.rdd.partitions.length / 30))
  }

}
