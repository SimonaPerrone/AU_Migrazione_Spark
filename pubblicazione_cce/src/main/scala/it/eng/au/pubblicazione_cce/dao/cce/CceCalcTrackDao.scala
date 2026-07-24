package it.eng.au.pubblicazione_cce.dao.cce

import it.eng.au.pubblicazione_cce.dao.HiveDao
import it.eng.au.pubblicazione_cce.model.cce.CceCalcTrackModel
import it.eng.au.pubblicazione_cce.schema.cce.CceCalcTrackSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, lit, lpad, when}

class CceCalcTrackDao extends HiveDao[CceCalcTrackModel] {
  private val spark = Environment.getSpark

  import spark.implicits._

  override val tableName: String = Environment.getCceCalcTrackTableName
  override val columns: List[String] = CceCalcTrackSchema.getValues

  override def read(): Dataset[CceCalcTrackModel] = {
    super.read()
      .withColumn(CceCalcTrackSchema.t_mese_calc, when(col(CceCalcTrackSchema.t_mese_calc).isNull, lit(null))
        .otherwise(lpad(col(CceCalcTrackSchema.t_mese_calc), 2, "0")))
      .as[CceCalcTrackModel]
  }
}
