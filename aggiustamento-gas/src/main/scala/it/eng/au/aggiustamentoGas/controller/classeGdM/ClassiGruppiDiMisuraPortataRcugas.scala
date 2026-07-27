package it.eng.au.aggiustamentoGas.controller.classeGdM

import it.eng.au.aggiustamentoGas.schema.agg.ClassiGruppiDiMisuraPortataSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, trim}

class ClassiGruppiDiMisuraPortataRcugas {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  val csvPathToLoad: String = Environment.getClassGroupMeasureRangeMaxPath

  @deprecated("La sterilizzazione dei consumi è affidata al processo di pubblicazione (vedere progetto aggregatore-consumi-agg).")
  def get: RDD[(String,Int)] = {
    val classiGruppiDiMisuraPortata =
      Environment.getSpark.sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .schema(ClassiGruppiDiMisuraPortataSchema.createSparkSchema())
        .load(csvPathToLoad)
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
        .cache

    logger.warn(s"count di classi gruppo di misura portata: ${classiGruppiDiMisuraPortata.count}")

    classiGruppiDiMisuraPortata
  }

}
