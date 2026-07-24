package it.eng.au.pubblicazione_cce.file.csv

import it.eng.au.pubblicazione_cce.common.CostantiCCE
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, concat_ws, lit, when}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

// Implementa i metodi generici per tutti gli output CCE
abstract class CceCsvBuilder extends DataFrameCsvBuilder {

  val outputFilePath: String = Environment.getOutputFilePath
  val dataCalcolo: LocalDate = Environment.processDate
  val fileTimestamp: String = Environment.fileTimestamp

  // parametri per file path
  // queste colonne devono essere sempre presenti nella lista columnsFileGroup
  val pivaCol: String
  val processoCol: String
  val ruoloCol: String

  val annoCalcolo: String = dataCalcolo.getYear.toString
  val meseCalcolo: String = dataCalcolo.format(DateTimeFormatter.ofPattern("MM"))


  override val maxLineCsv: Int = 0

  override def computePathRoot: Column = lit(outputFilePath)

  override def computeSubDirectories: Column = {
    concat_ws(PATH_SEPARATOR,
      lit(CostantiCCE.CCE),
      when(col(ruoloCol) === CostantiCCE.RUOLO_UDD, lit(CostantiCCE.PATH_UDD))
        .otherwise(lit(CostantiCCE.PATH_ID)),
      col(pivaCol),
      col(processoCol),
      lit(annoCalcolo),
      lit(meseCalcolo),
      lit("") // aggiunge '/' alla fine del percorso
    )
  }

}
