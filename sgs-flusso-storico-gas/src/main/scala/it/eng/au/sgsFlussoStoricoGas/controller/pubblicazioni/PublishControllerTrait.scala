package it.eng.au.sgsFlussoStoricoGas.controller.pubblicazioni

import it.eng.au.sgsFlussoStoricoGas.model.{PubListModel, XmlOutputModel}
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import it.eng.au.sgsFlussoStoricoGas.utility.file.FileUtility.set777
import org.apache.spark.sql.{DataFrame, Dataset}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.nio.file.{Files, Paths}
import java.io.IOException
import java.nio.charset.StandardCharsets
import scala.util.Try

trait PublishControllerTrait extends Serializable {

  val nomeServizio = ""

  private def writeFile(outputDir: String, xmlFileName: String, xmlContent: String): Unit = {
    // Scrive il file XML su disco
    try {
      val outputPath = Paths.get(outputDir)

      // Se la directory non esiste, la funzione termina subito
      if (!Files.exists(outputPath)) {
        println(s"Directory $outputDir non esistente, nessuna scrittura eseguita.")
        return
      }

      val xmlFilePath = outputPath.resolve(xmlFileName)

      // Imposta i permessi sulle cartelle
      Try(set777(outputPath.getParent.getParent.toFile))
      set777(outputPath.getParent.toFile) // ANNO
      set777(outputPath.toFile) // MESE

      // Scrive il contenuto XML su disco
      println(s"Writing XML to $xmlFilePath")
      Files.write(xmlFilePath, xmlContent.getBytes(StandardCharsets.UTF_8))

      // Imposta i permessi sul file XML
      set777(xmlFilePath.toFile)
    } catch {
      case ex: IOException =>
        println(s"Errore durante la scrittura del file XML: ${ex.getMessage}")
    }
  }

  private def generateXmlContent(group: List[XmlOutputModel], pivaUtente: String): String = {
    s"""<?xml version="1.0" encoding="utf-8"?>
       |<FlussoSGS xmlns:xs="http://www.w3.org/2001/XMLSchema" cod_flusso="SGS">
       |\t<piva_utente>$pivaUtente</piva_utente>
       |${
      group.groupBy(_.cod_pdr).map { case (codPdr, pdrRecords) =>
        val record = pdrRecords.head // Dati funzionali identici, prendiamo il primo

        val datiFunzionali = {
          val elementi = Seq(
            Option(record.matr_mis).filter(_.nonEmpty).map(v => s"\t\t<matr_mis>$v</matr_mis>"),
            Option(record.data_inst_mis).filter(_.nonEmpty).map(v => s"\t\t<data_inst_mis>$v</data_inst_mis>"),
            Option(record.gruppo_mis_int).filter(_.nonEmpty).map(v => s"\t\t<gruppo_mis_int>$v</gruppo_mis_int>"),
            Option(record.classe_gruppo_mis).filter(_.nonEmpty).map(v => s"\t\t<classe_gruppo_mis>$v</classe_gruppo_mis>"),
            Option(record.telegestione).filter(_.nonEmpty).map(v => s"\t\t<telegestione>$v</telegestione>"),
            Option(record.pre_conv).filter(_.nonEmpty).map(v => s"\t\t<pre_conv>$v</pre_conv>"),
            Option(record.matr_conv).filter(_.nonEmpty).map(v => s"\t\t<matr_conv>$v</matr_conv>"),
            Option(record.coeff_cor).filter(_.nonEmpty).map(v => s"\t\t<coeff_corr>$v</coeff_corr>"),
            Option(record.data_inst_conv).filter(_.nonEmpty).map(v => s"\t\t<data_inst_conv>$v</data_inst_conv>"),
            Option(record.cod_remi).filter(_.nonEmpty).map(v => s"\t\t<cod_remi>$v</cod_remi>"),
            Option(record.id_reg_clim).filter(_.nonEmpty).map(v => s"\t\t<id_reg_clim>$v</id_reg_clim>"),
            Option(record.tipo_mis).filter(_.nonEmpty).map(v => s"\t\t<tipo_mis>$v</tipo_mis>"),
            Some(s"\t\t<cod_pdr>$codPdr</cod_pdr>")
          ).flatten

          s"<dati_funz>\n${elementi.mkString("\n")}"
        }

        val datiConsumo =
          pdrRecords.map { record =>
            s"""\t<DatiPdr>
               |\t\t<MeseAnno>${record.mese_anno}</MeseAnno>
               |\t<DatiPdRTecn>
               |\t\t<Trattamento>${record.trattamento}</Trattamento>
               |\t\t<piva_distr>${record.piva_distr}</piva_distr>
               |\t</DatiPdRTecn>
               |\t<DatiConsumo>
               |\t\t<Prel_Aggregato>${record.prel_aggregato}</Prel_Aggregato>
               |${
              List(
                Option(record.prelievo_1),
                Option(record.prelievo_2),
                Option(record.prelievo_3),
                Option(record.prelievo_4),
                Option(record.prelievo_5),
                Option(record.prelievo_6),
                Option(record.prelievo_7),
                Option(record.prelievo_8),
                Option(record.prelievo_9),
                Option(record.prelievo_10),
                Option(record.prelievo_11),
                Option(record.prelievo_12),
                Option(record.prelievo_13),
                Option(record.prelievo_14),
                Option(record.prelievo_15),
                Option(record.prelievo_16),
                Option(record.prelievo_17),
                Option(record.prelievo_18),
                Option(record.prelievo_19),
                Option(record.prelievo_20),
                Option(record.prelievo_21),
                Option(record.prelievo_22),
                Option(record.prelievo_23),
                Option(record.prelievo_24),
                Option(record.prelievo_25),
                Option(record.prelievo_26),
                Option(record.prelievo_27),
                Option(record.prelievo_28),
                Option(record.prelievo_29),
                Option(record.prelievo_30),
                Option(record.prelievo_31)
              ).zipWithIndex.collect {
                case (Some(value), index) if value.nonEmpty =>
                  s"""\t\t<Prelievo Giorno="${"%02d".format(index + 1)}">$value</Prelievo>"""
              }.mkString("\n")
            }
               |\t</DatiConsumo>
               |\t</DatiPdr>""".stripMargin
          }.mkString("\n")
        s"$datiFunzionali\n$datiConsumo\n\t</dati_funz>"
      }.mkString("\n")
    }
       |</FlussoSGS>
       |""".stripMargin
  }

  def writeXml(df: DataFrame, outputPathPrefix: String, pdrPerFile: Int): List[PubListModel] = {
    val SQLContext = Environment.getSpark.sqlContext
    import SQLContext.implicits._

    //Genera timestamp
    val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
    val timestamp = LocalDateTime.now().format(dateFormatter)

    // Raggruppa i dati per `piva_utente`
    val typedDataset: Dataset[XmlOutputModel] = df.as[XmlOutputModel]
    val groupedDataset = typedDataset.groupByKey(row => (row.piva_utente, row.mese_anno_decorr, row.mese_anno_pub))

    // Raccoglie i risultati in una lista
    val resultList = groupedDataset.flatMapGroups { case ((pivaUtente, annoMeseDecorr, annoMesePub), rows) =>
      val rowsList = rows.toList.sortBy(row => (row.cod_pdr, (row.mese_anno.split("/")(1)+row.mese_anno.split("/")(0)).toInt)) // Ordina per pdr, annomese
      val groupedByPdr = rowsList.groupBy(_.cod_pdr).values.toList // Raggruppa tutti i dati per cod_pdr mantenendo tutte le informazioni per annomese
      val groupedList = groupedByPdr.grouped(pdrPerFile).map(_.flatten).toList

      // Creiamo una lista di file XML per ZIP
      val xmlFiles = groupedList.zipWithIndex.map { case (group, groupIndex) =>

        // Definisci i nomi e i path
        val annoDecorr = annoMeseDecorr.substring(0,4)
        val meseDecorr = annoMeseDecorr.substring(4,6)
        val outputDir = s"$outputPathPrefix/$nomeServizio/${nomeServizio}_$pivaUtente/$annoDecorr/$meseDecorr"
        val outputFileName = s"${pivaUtente}_${annoMesePub}_SGS_$timestamp"
        val xmlFileName = s"${outputFileName}_${groupIndex + 1}.xml"
        val zipFileName = s"${outputFileName}_${groupIndex + 1}.zip"
        val zipFilePath = Paths.get(outputDir, zipFileName).toString

          // Genera il contenuto XML
          val xmlContent = generateXmlContent(group, pivaUtente)

          writeFile(outputDir, xmlFileName, xmlContent)

          (outputDir, xmlFileName, zipFileName, zipFilePath, group.map(_.cod_pdr).mkString(","), groupIndex+1)
        // Ritorna il path e il nome del file XML, i pdr coinvolti e gli index dei file xml
        }

      // Ritorna le informazioni
      xmlFiles.map { case (outputDir, xmlFileName, zipFileName, zipFilePath, codPdrs, index) =>
          PubListModel(pivaUtente, outputDir, xmlFileName, codPdrs, zipFilePath, zipFileName, index)
        }
      }.rdd.collect.toList

    resultList
  }
}
