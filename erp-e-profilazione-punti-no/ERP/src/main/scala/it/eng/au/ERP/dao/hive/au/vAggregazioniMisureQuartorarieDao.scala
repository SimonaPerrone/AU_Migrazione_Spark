package it.eng.au.ERP.dao.hive.au

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.au.{vAggregazioniMisureQuartorarieSchema}
import it.eng.au.ERP.utility.setting.PropertyUtility
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.AnalysisException

class vAggregazioniMisureQuartorarieDao extends HDao{

  override val tableName: String = PropertyUtility.vAggregazioniMisureQuartorarieTable
  override val schema: SchemaEnum = vAggregazioniMisureQuartorarieSchema
  override val writeEnabled: Boolean = false

  override def read()(implicit spark: SparkSession): DataFrame = {
    try {
      super.read()
    } catch {
      case _: AnalysisException =>
        spark.sqlContext.read
          .table(PropertyUtility.aggregazioniMisureQuartorarieTable)
          .selectExpr(columns: _*)
    }
  }
}
