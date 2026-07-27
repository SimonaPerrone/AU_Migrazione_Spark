package it.eng.au.freezerPreCalcolo.dao.rcugas

import it.eng.au.freezerPreCalcolo.schema.RcuGasMassivoFrozenSchema

import it.eng.au.freezerPreCalcolo.utility.environment.Environment

class RcuGasMassivoFrozenDAO extends FrozenDAO {
  override val hdfsOutput: String = Environment.getRcugasMassivoFreeze
  override val partitionCols: List[String] = List(RcuGasMassivoFrozenSchema.session, RcuGasMassivoFrozenSchema.execution_id)
  override val columns: List[String] = RcuGasMassivoFrozenSchema.getValues
  val tableName: String = Environment.getRcugasMassivoTableName
}
