package it.eng.au.freezerPreCalcolo.dao.rcugas

import it.eng.au.freezerPreCalcolo.schema.RcuGasConnessioniDistr2Schema

import it.eng.au.freezerPreCalcolo.utility.environment.Environment

class RcuGasConnessioniDistr2DAO extends DAO {
  override val parquetPath: String = Environment.getRcuGasConnessioniDistr2
  override val columns: List[String] = RcuGasConnessioniDistr2Schema.getValues
}
