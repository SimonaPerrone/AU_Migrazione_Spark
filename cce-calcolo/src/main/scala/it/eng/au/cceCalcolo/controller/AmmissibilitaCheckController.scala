package it.eng.au.cceCalcolo.controller

import it.eng.au.cceCalcolo.schema.ammissibilita.ReportAmmissibilitaPodSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, hash, upper}
import org.apache.spark.sql.types.StringType

class AmmissibilitaCheckController {

  def getReportAmmissibilitaUnion (ReportPodDF: DataFrame, ReportFileDF: DataFrame) : DataFrame = {
    ReportPodDF.unionByName(ReportFileDF)
      .distinct
      .withColumn("hashKey", hash(concat(upper(col(ReportAmmissibilitaPodSchema.nome_file)),col(ReportAmmissibilitaPodSchema.pod), col(ReportAmmissibilitaPodSchema.annomese).cast(StringType), col(ReportAmmissibilitaPodSchema.annomesegiornodir))))
  }

}
