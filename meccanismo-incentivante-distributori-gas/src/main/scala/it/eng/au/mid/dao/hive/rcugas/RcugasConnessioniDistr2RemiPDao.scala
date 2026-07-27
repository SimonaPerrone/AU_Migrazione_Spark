package it.eng.au.mid.dao.hive.rcugas

import it.eng.au.mid.dao.hive.HiveDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.hive.rcugas.RcugasConnessioniDistr2RemiPModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.hive.rcugas.RcugasConnessioniDistr2RemiPSchema
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

import java.sql.Timestamp

class RcugasConnessioniDistr2RemiPDao extends HiveDao[RcugasConnessioniDistr2RemiPModel] {
  override val tableName: String = Environment.getProperty("hive.table.rcugas_connessioni_distr2_remi_p")
  override val schema: SchemaEnum = RcugasConnessioniDistr2RemiPSchema

  /***
   * Legge righe con valori di inizio-fine data_conn e data_aggregazione che comprendono il valore Timestamp
   * passato in parametro
   */
  def readConnessioniAttive(ts: Timestamp): Dataset[RcugasConnessioniDistr2RemiPModel] = {
    read()
      .where(col(RcugasConnessioniDistr2RemiPSchema.d_data_inizio_conn).isNull or col(RcugasConnessioniDistr2RemiPSchema.d_data_inizio_conn) <= ts)
      .where(col(RcugasConnessioniDistr2RemiPSchema.d_data_fine_conn).isNull or col(RcugasConnessioniDistr2RemiPSchema.d_data_fine_conn) > ts)
      .where(col(RcugasConnessioniDistr2RemiPSchema.d_data_inizio_aggregazione).isNull or col(RcugasConnessioniDistr2RemiPSchema.d_data_inizio_aggregazione) <= ts)
      .where(col(RcugasConnessioniDistr2RemiPSchema.d_data_fine_aggregazione).isNull or col(RcugasConnessioniDistr2RemiPSchema.d_data_fine_aggregazione) > ts)
  }
}
