package it.eng.au.ERP.dao.hive.erp

import it.eng.au.ERP.dao.hive.HDao
import it.eng.au.ERP.model.erp.erpConsumptionNoModel
import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.schema.erp.{erpConsumptionNoSchema, erpValidatedIntSchema, erpValidatedMisNoSchema}
import it.eng.au.ERP.utility.setting.PropertyUtility
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

class ErpValidatedMisNODao extends HDao{
  override val tableName: String = PropertyUtility.erpValidatedMisNo
  override val schema: SchemaEnum = erpValidatedMisNoSchema


  def readByExecutionId(executionId: String)(implicit spark: SparkSession): Dataset[erpConsumptionNoModel] = {
    import spark.implicits._

    spark.sqlContext.read
      .table(tableName)
      .filter(col(erpValidatedMisNoSchema.executionid) === executionId )
      .selectExpr(columns: _*)
      .as[erpConsumptionNoModel]
  }

}
