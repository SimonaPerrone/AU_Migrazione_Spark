package it.eng.au.ERP.dao.hive.erp

import it.eng.au.ERP.dao.hive.HiveDao
import it.eng.au.ERP.model.erp.erpConsumptionNoModel
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.erp.erpConsumptionNoSchema
import it.eng.au.ERP.utility.setting.PropertyUtility
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class ErpConsumptionNODao extends HiveDao[erpConsumptionNoModel] {
  override val tableName: String = PropertyUtility.erpConsumptionNo
  override val schema: SchemaEnum = erpConsumptionNoSchema

  def readByExecutionId(executionId: String)(implicit spark: SparkSession): Dataset[erpConsumptionNoModel] = {
    import spark.implicits._

    spark.sqlContext.read
      .table(tableName)
      .filter(col(erpConsumptionNoSchema.executionid) === executionId)
      .selectExpr(columns: _*)
      .as[erpConsumptionNoModel]
  }

  def readByExecutionIdDf(executionId: String)(implicit spark: SparkSession): DataFrame = {
    spark.sqlContext.read
      .table(tableName)
      .filter(col(erpConsumptionNoSchema.executionid) === executionId)
      .selectExpr(columns: _*)
  }
}