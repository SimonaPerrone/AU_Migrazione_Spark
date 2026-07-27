package it.eng.au.portaleConsumi.dao.hive.rcugas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.rcugas.RcugasFornituraPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.rcugas.RcugasFornituraPSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

import java.sql.Timestamp

class RcugasFornituraPDao() extends HiveDao[RcugasFornituraPModel] {
  override val tableName: String = Environment.getProperty("hive.table.rcugas_fornitura_p")
  override val schema: SchemaEnum = RcugasFornituraPSchema

  def read(fromTimestamp: Timestamp): Dataset[RcugasFornituraPModel] = {
    read()
      .where(col(RcugasFornituraPSchema.d_data_fine).isNull or (col(RcugasFornituraPSchema.d_data_fine) >= fromTimestamp))
  }
}
