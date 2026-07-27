package it.eng.au.freezerPreCalcolo.dao.rcugas

import it.eng.au.freezerPreCalcolo.schema.RcuGasMassivoSchema

import it.eng.au.freezerPreCalcolo.utility.environment.Environment

class RcuGasMassivoDAO extends DAO {
  override val parquetPath: String = Environment.getAggRcuGasMassivo
  override val columns: List[String] = RcuGasMassivoSchema.getValues
}
