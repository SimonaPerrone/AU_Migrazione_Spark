package it.eng.au.calcoloSettlementGas.dao

import it.eng.au.calcoloSettlementGas.dao.`trait`.DAO
import it.eng.au.calcoloSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.schema.TSGQKRIUDSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class TSGQKRIUDDao extends DAO {
  override val tableName: String = Properties.getTSG2TSGQKRIUDTableName
  override val columns: List[String] = TSGQKRIUDSchema.getValues
  override val partitionColumn: String = TSGQKRIUDSchema.annomese

  override def readTablePartiton(partition: String): DataFrame = {
    readTable.filter(col(partitionColumn) === partition)
      .withColumn(TSGQKRIUDSchema.qkriud, col(TSGQKRIUDSchema.qkriud) * 100)
  }
}
