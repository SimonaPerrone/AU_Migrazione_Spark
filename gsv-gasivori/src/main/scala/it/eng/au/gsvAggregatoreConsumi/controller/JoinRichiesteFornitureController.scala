package it.eng.au.gsvAggregatoreConsumi.controller

import it.eng.au.gsvAggregatoreConsumi.schema.gsv.{GsvConsFornitureSchema, GsvConsRichiestaSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DateType

class JoinRichiesteFornitureController {

  def JoinRichiesteForniture(richiesteDF: DataFrame, fornitureDF: DataFrame) : DataFrame = {
    val fornitureJoined = fornitureDF
      .join(richiesteDF, Seq(GsvConsFornitureSchema.n_id_gsv5_cons_richiesta.toString), "inner")
      .withColumn(GsvConsFornitureSchema.d_data_inizio, col(GsvConsFornitureSchema.d_data_inizio).cast(DateType))
      .withColumn(GsvConsFornitureSchema.d_data_fine, col(GsvConsFornitureSchema.d_data_fine).cast(DateType))
      .withColumn(GsvConsRichiestaSchema.d_data_richiesta, col(GsvConsRichiestaSchema.d_data_richiesta).cast(DateType))

    fornitureJoined
  }
}
