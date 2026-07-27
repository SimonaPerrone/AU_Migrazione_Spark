package it.eng.au.sgsFlussoStoricoGas.dao.rcugas

import it.eng.au.sgsFlussoStoricoGas.dao.Dao
import it.eng.au.sgsFlussoStoricoGas.schema.rcugas.RcuGasPdrPSchema
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame

class RcuGasPdrPDao extends Dao {
  override val tablePath: String = Environment.getRcugasPdrPath
  override val columns: List[String] = RcuGasPdrPSchema.getValues

  override def readTable: DataFrame = {
    super.readTable
      .selectExpr(columns:_*)
  }
}
