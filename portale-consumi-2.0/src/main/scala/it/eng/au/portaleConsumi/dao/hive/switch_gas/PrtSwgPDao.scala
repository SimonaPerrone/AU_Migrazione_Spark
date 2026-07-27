package it.eng.au.portaleConsumi.dao.hive.switch_gas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.switch_gas.PrtSwgPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.switch_gas.PrtSwgPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, min, not}

import java.sql.Timestamp

class PrtSwgPDao extends HiveDao[PrtSwgPModel] {
  override val tableName: String = Environment.getProperty("hive.table.switch_gas_prt_swg_p")
  override val schema: SchemaEnum = PrtSwgPSchema

  /*
  Ritorna dataframe [t_codice_pdr, d_data_decorrenza] per ogni PDR per cui esiste uno switch successivo a fromTimestamp,
  con data minima di decorrenza
   */
  def readProssimiSwitchPDR(fromTimestamp: Timestamp): DataFrame = {
    read()
      .where(not(col(PrtSwgPSchema.t_stato).isin("B", "TE2", "TE3", "E1", "E2", "E3")))
      .where(col(PrtSwgPSchema.d_data_decorrenza) >= fromTimestamp)
      .groupBy(PrtSwgPSchema.t_codice_pdr)
      .agg(min(PrtSwgPSchema.d_data_decorrenza) as PrtSwgPSchema.d_data_decorrenza)
  }
}
