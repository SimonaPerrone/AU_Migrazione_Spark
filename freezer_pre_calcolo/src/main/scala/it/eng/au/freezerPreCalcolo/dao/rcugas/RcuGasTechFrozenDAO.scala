package it.eng.au.freezerPreCalcolo.dao.rcugas

import it.eng.au.freezerPreCalcolo.schema.RcuGasTechFrozenSchema

import it.eng.au.freezerPreCalcolo.utility.environment.Environment

class RcuGasTechFrozenDAO extends FrozenDAO {
  override val hdfsOutput: String = Environment.getRcugasTechFreeze
  override val partitionCols: List[String] = List(RcuGasTechFrozenSchema.session, RcuGasTechFrozenSchema.execution_id)
  override val columns: List[String] = RcuGasTechFrozenSchema.getValues
  val tableName: String = Environment.getRcugasTechTableName

}
