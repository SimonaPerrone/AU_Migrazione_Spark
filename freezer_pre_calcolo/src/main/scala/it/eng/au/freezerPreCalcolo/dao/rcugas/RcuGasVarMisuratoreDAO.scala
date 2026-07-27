package it.eng.au.freezerPreCalcolo.dao.rcugas

import it.eng.au.freezerPreCalcolo.schema.RcuGasVarMisuratoreSchema

import it.eng.au.freezerPreCalcolo.utility.environment.Environment

class RcuGasVarMisuratoreDAO extends DAO {
  override val parquetPath: String = Environment.getRcuGasVarMisurratore
  override val columns: List[String] = RcuGasVarMisuratoreSchema.getValues
}
