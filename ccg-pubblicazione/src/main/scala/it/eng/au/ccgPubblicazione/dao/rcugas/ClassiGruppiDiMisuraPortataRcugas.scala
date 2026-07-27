package it.eng.au.ccgPubblicazione.dao.rcugas

import it.eng.au.ccgPubblicazione.schema.rcugas.ClassiGruppiDiMisuraPortataSchema
import it.eng.au.ccgPubblicazione.utility.Environment
import org.apache.spark.sql.functions.{col, trim}

object ClassiGruppiDiMisuraPortataRcugas {
  val csvPathToLoad: String = Environment.getClassGroupMeasureRangeMaxPath

  def getAsMap(): Map[String, Int] = {
    val classiGruppiDiMisuraPortata =
      Environment.sqlContext.read
        .option("header", "true")
        .option("delimiter", ";")
        .schema(ClassiGruppiDiMisuraPortataSchema.createSparkSchema())
        .csv(csvPathToLoad)
        .select(
          trim(col(ClassiGruppiDiMisuraPortataSchema.GRUPPO_DI_MISURA)).as(ClassiGruppiDiMisuraPortataSchema.GRUPPO_DI_MISURA),
          trim(col(ClassiGruppiDiMisuraPortataSchema.PORTATA_MAX)).as(ClassiGruppiDiMisuraPortataSchema.PORTATA_MAX)
        )
        .distinct
        .rdd
        .map(r => {
          (
            r.getAs[String](ClassiGruppiDiMisuraPortataSchema.GRUPPO_DI_MISURA),
            r.getAs[String](ClassiGruppiDiMisuraPortataSchema.PORTATA_MAX).toInt
          )
        })

    classiGruppiDiMisuraPortata.collectAsMap().toMap
  }
}
