package it.eng.au.sgsFlussoStoricoGas.dao.prt

import it.eng.au.sgsFlussoStoricoGas.dao.Dao
import it.eng.au.sgsFlussoStoricoGas.schema.prt.PrtIstatRegioneClimaticaPSchema
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame

class PrtIstatRegioneClimaticaPDao extends Dao {
  override val tablePath: String = Environment.getPrtIstatRegioneClimaticaPath
  override val columns: List[String] = PrtIstatRegioneClimaticaPSchema.getValues

  def get(rcuGasPdrP: DataFrame): DataFrame = {
    readTable.join(rcuGasPdrP, Seq(PrtIstatRegioneClimaticaPSchema.t_codice_istat.toString), "left")
  }
}
