package it.eng.au.cceCalcolo.dao.ammissibilita

import it.eng.au.cceCalcolo.schema.ammissibilita.ReportAmmissibilitaFileSchema
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, upper}

class ReportAmmissibilitaFileDAO extends ReportAmmissibilitaDAO {
  override val tablePath: String = Properties.getReportAmmissibilitaFilePath
  override val tableName: String = Properties.getReportAmmissibilitaFileTable
  override val columns: List[String] = ReportAmmissibilitaFileSchema.getValues

  def getReportFile: DataFrame = {
    getReportAmmissibilita
      .withColumn(ReportAmmissibilitaFileSchema.nome_file, upper(col(ReportAmmissibilitaFileSchema.nome_file)))
      .withColumn("pod", lit(""))
      .filter(col(ReportAmmissibilitaFileSchema.codice_inamissibilita) =!= "919")
      .selectExpr(ReportAmmissibilitaFileSchema.nome_file.toString, "pod", "annomese", ReportAmmissibilitaFileSchema.annomesegiornodir, ReportAmmissibilitaFileSchema.flusso)
      .distinct
  }
}
