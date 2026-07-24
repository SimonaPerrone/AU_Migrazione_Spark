package it.eng.au.pubblicazione_cce.dao.cce

import it.eng.au.pubblicazione_cce.dao.HiveDao
import it.eng.au.pubblicazione_cce.model.cce.CceCalcoloMisureModel
import it.eng.au.pubblicazione_cce.schema.cce.CceCalcoloMisureSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, lpad}

// Interfaccia per leggere le tabelle di misura (P,Pein,PR,PRein) che hanno tutte lo schema in comune
trait CceCalcoloDao extends HiveDao[CceCalcoloMisureModel] {
  private val spark = Environment.getSpark

  import spark.implicits._

  override val columns: List[String] = CceCalcoloMisureSchema.getValues

  override def read(): Dataset[CceCalcoloMisureModel] = {
    super.read()
      // mese a doppia cifra
      .withColumn(CceCalcoloMisureSchema.mese, lpad(col(CceCalcoloMisureSchema.mese), 2, "0"))
      .as[CceCalcoloMisureModel]
  }

}
