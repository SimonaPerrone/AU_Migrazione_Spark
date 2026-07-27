package it.eng.au.portaleConsumi.dao.hive.misuregas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.misuregas.MisureStoricF2Model
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.misuregas.MisureStoricF2Schema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset

class MisureStoricF2Dao() extends HiveDao[MisureStoricF2Model] {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)
  override val tableName: String = Environment.getProperty("hive.table.misure_storic_f2")
  override val schema: SchemaEnum = MisureStoricF2Schema

  def write(data: Dataset[MisureStoricF2Model]): Unit = {
    super.write(data, overwrite = true)
  }

}

