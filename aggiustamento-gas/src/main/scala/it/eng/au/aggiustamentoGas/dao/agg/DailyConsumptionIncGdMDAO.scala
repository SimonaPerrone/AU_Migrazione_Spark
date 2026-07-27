package it.eng.au.aggiustamentoGas.dao.agg

import it.eng.au.aggiustamentoGas.schema.agg.DailyConsumptionIncoerentiGdMSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment

class DailyConsumptionIncGdMDAO extends AggDao {
  override val tableName: String = Environment.getDailyConsumptionIncGdMTable
  override val parquetPath: String = Environment.getDailyConsumptionIncGdMPath
  override val columns: List[String] = DailyConsumptionIncoerentiGdMSchema.getValues

}
