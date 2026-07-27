package it.eng.au.calcoloSettlementGas.dao

import it.eng.au.calcoloSettlementGas.dao.`trait`.DAO
import it.eng.au.calcoloSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.schema.TSGTFCSchema

class TSGTFCDao extends DAO {
  override val tableName: String = Properties.getTSG2TSGTFCTableName
  override val columns: List[String] = TSGTFCSchema.getValues
  override val partitionColumn: String = TSGTFCSchema.annomese
}
