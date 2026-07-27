package it.eng.cdp_codprofstd_tds.dao.agg

import it.eng.cdp_codprofstd_tds.dao.OutputDao
import it.eng.cdp_codprofstd_tds.schema.CodProfStdDaTdsSchema
import it.eng.cdp_codprofstd_tds.utility.Environment

class CodProfStdDaTdsDao extends OutputDao {
  override val tableName: String = Environment.getCodPrfStdDaTdsTableName
  override val hdfsOutput: String = Environment.getCodProfStdDaTds
  override val partitionCols: List[String] = List(CodProfStdDaTdsSchema.anno_competenza, CodProfStdDaTdsSchema.execution_id)
  override val columns: List[String] = CodProfStdDaTdsSchema.getValues
}
