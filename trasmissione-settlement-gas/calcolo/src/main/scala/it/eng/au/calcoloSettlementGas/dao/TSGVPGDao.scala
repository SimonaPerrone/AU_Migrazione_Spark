package it.eng.au.calcoloSettlementGas.dao

import it.eng.au.calcoloSettlementGas.dao.`trait`.DAO
import it.eng.au.calcoloSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.schema.TSGVPGSchema

class TSGVPGDao extends DAO {
  override val tableName: String = Properties.getTSG2TSGVPGTableName
  override val partitionColumn: String = TSGVPGSchema.annomese
  override val columns: List[String] = TSGVPGSchema.getValues
}
