package it.eng.au.cceCalcolo.dao.ammissibilita

import it.eng.au.cceCalcolo.schema.ammissibilita.ReportAmmissibilitaPodSchema
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, substring, upper}

class ReportAmmissibilitaPodDAO extends ReportAmmissibilitaDAO {
  override val tablePath: String = Properties.getReportAmmissibilitaPodPath
  override val tableName: String = Properties.getReportAmmissibilitaPodTable
  override val columns: List[String] = ReportAmmissibilitaPodSchema.getValues

  def getReportPod: DataFrame = {
    getReportAmmissibilita
      .withColumn(ReportAmmissibilitaPodSchema.nome_file, upper(col(ReportAmmissibilitaPodSchema.nome_file)))
      .withColumn(ReportAmmissibilitaPodSchema.pod, substring(col(ReportAmmissibilitaPodSchema.pod),1,14))
      .selectExpr(ReportAmmissibilitaPodSchema.nome_file.toString, "pod", "annomese", ReportAmmissibilitaPodSchema.annomesegiornodir, ReportAmmissibilitaPodSchema.flusso)
      .distinct
  }
}
