package it.eng.au.aggregatoreConsumiCommon.dao

import it.eng.au.aggregatoreConsumiCommon.schema.ClassiGruppiDiMisuraPortataSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, trim}

/** Data Access Object per leggere la tabella statica contenente la relazione tra gruppo di misura e portata massima. */
object ClassiGruppiDiMisuraPortataRcugas {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

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
