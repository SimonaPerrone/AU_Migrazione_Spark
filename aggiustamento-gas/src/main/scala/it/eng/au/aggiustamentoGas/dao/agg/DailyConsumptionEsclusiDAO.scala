package it.eng.au.aggiustamentoGas.dao.agg

import it.eng.au.aggiustamentoGas.schema.agg.DailyConsumptionEsclusiSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment

class DailyConsumptionEsclusiDAO extends AggDao {
  override val tableName: String = Environment.getDailyConsumptionExclTable
  override val parquetPath: String = Environment.getDailyConsumptionExclPath
  override val columns: List[String] = DailyConsumptionEsclusiSchema.getValues

}
