package it.eng.au.freezerPreCalcolo.dao.rcugas

import it.eng.au.freezerPreCalcolo.schema.RcuGasVarConvertitoreSchema

import it.eng.au.freezerPreCalcolo.utility.environment.Environment

class RcuGasVarConvertitoreDAO extends DAO {
  override val parquetPath: String = Environment.getRcuGasVarConvertitore
  override val columns: List[String] = RcuGasVarConvertitoreSchema.getValues
}
