package it.eng.au.ammissibilitaRendiconti.controller

import it.eng.au.ammissibilitaRendiconti.model._
import it.eng.au.ammissibilitaRendiconti.model.rules.{CsvRule, ZipRule}
import it.eng.au.ammissibilitaRendiconti.schema.Rzg1CsvSchema
import it.eng.au.ammissibilitaRendiconti.utility.constants.Constants._
import it.eng.au.ammissibilitaRendiconti.utility.environment.Properties
import org.joda.time.format.DateTimeFormat

import java.io.{BufferedReader, IOException, InputStreamReader, UnsupportedEncodingException}
import java.util.stream.Collectors
import java.util.zip.ZipFile
import scala.collection.JavaConverters.enumerationAsScalaIteratorConverter
import scala.util.Try

/** Implementa una serie di funzioni per l'estrazione dei file CSV dagli ZIP, il parsing di questi, e la successiva ammissibilità. */
object ReadCsvController extends Serializable {

  /**
   *  1. Esegue i check di ammissibilità dei file ZIP
   *  1. Decomprime i CSV presenti nei file ZIP ammissibili
   *  1. Esegue i check di ammissibilità dei file CSV decompressi */
  def checkAndUnzip(zipMeta: ZipRzg1Metadata, zipRules: List[ZipRule], csvRules: List[CsvRule], isRuleCheckNumberOfCsvFieldsEnabled: Boolean): ZipRzg1Metadata = {
    val zipMetaWithInfo = checkZip(zipMeta, zipRules)
    if (!zipMetaWithInfo.isAmmissibile) return zipMetaWithInfo

    val zipMetaWithCsv = unzipCsv(zipMetaWithInfo, isRuleCheckNumberOfCsvFieldsEnabled)
    if (!zipMetaWithCsv.isAmmissibile) return zipMetaWithCsv

    checkCsv(zipMetaWithCsv, csvRules)
  }

  /** Esegue sui file ZIP letti i controlli delle regole di ammissibilità attive. */
  def checkZip(zipMeta: ZipRzg1Metadata, zipRules: List[ZipRule]): ZipRzg1Metadata = {
    /** Messaggio di ammissibilità positiva. */
    val okMessage = ReportMessage()
    /** Prima regola, se esiste e in ordine di priorità, che non viene rispettata dal file [[zipMeta]]. */
    val errorRule = zipRules.find(rule => rule.isEnabled && !rule.condition(zipMeta))
    /** Messaggio di errore da associare al file [[zipMeta]]. */
    val message = if (errorRule.isDefined) errorRule.get.message else okMessage

    zipMeta.copy(
      isAmmissibile = message.isAmmissibile,
      statusCode = message.statusCode,
      statusMessage = message.statusMessage
    )
  }

  /** Effettua il check di integrità del file ZIP. */
  def ruleCheckZipIntegrity: ZipRule = ZipRule(
    ruleName = "ruleCheckZipIntegrity",
    condition = zipMeta => {
      try {
        new ZipFile(zipMeta.file)
        true
      } catch {
        case _: IOException => false
      }
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_904,
      statusMessage = MOTIVAZIONE_ZIP_CORROTTO)
  )


  /** Controlla che all'interno dello ZIP ci sia un solo file e che questo file sia un file CSV. */
  def ruleCheckZipBody: ZipRule = ZipRule(
    ruleName = "ruleCheckZipBody",
    condition = zipMeta => {
      val zipFile = new ZipFile(zipMeta.file)

      zipFile.entries.asScala.toList.length == 1 &&
        !zipFile.entries.asScala.toList.head.isDirectory &&
        zipFile.entries.asScala.toList.head.getName.toLowerCase.endsWith(".csv")
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_904,
      statusMessage = MOTIVAZIONE_ZIP_NON_CONFORME)
  )

  /** Controlla l'uguaglianza tra il nome del file ZIP e il nome del file CSV contenuto, a meno delle estensioni. */
  def ruleCheckZipAndCsvNames: ZipRule = ZipRule(
    ruleName = "ruleCheckZipAndCsvNames",
    isEnabled = Properties.isRuleCheckZipAndCsvNamesEnabled,
    condition = zipMeta => {
      val zipFile = new ZipFile(zipMeta.file)
      val zipFileName = zipMeta.file.getName.toLowerCase.replace(".zip", "")
      val csvEntry = zipFile.entries.asScala.toList.head
      val csvFileName = csvEntry.getName.toLowerCase.replace(".csv", "")

      zipFileName.equals(csvFileName)
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_904,
      statusMessage = MOTIVAZIONE_CSV_NON_COERENTE
    )
  )

  /** Controlla che la partita iva nel nome dello ZIP corrisponda alla partita IVA nel percorso dove è presente lo ZIP. */
  def ruleCheckPiva: ZipRule = ZipRule(
    ruleName = "ruleCheckPiva",
    isEnabled = Properties.isRuleCheckPivaEnabled,
    condition = zipMeta => {
      val pivaFromPath = zipMeta.file.getParentFile.getParentFile.getParentFile.getName.split("_").last
      pivaFromPath == zipMeta.pivaId
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_904,
      statusMessage = MOTIVAZIONE_PIVAID_NON_COERENTE
    )
  )

  /** Controlla che il file CSV abbia l'encoding UTF-8. */
  def ruleCheckCsvEncoding: ZipRule = ZipRule(
    ruleName = "ruleCheckCsvEncoding",
    isEnabled = Properties.isRuleCheckCsvEncodingEnabled,
    condition = zipMeta => {
      try {
        val zipFile = new ZipFile(zipMeta.file)
        val entries = zipFile.entries.asScala.toList.head
        val csvInputStream = zipFile.getInputStream(entries)
        val csvReader = new BufferedReader(new InputStreamReader(csvInputStream, "UTF-8"))
        val lines = csvReader.lines().collect(Collectors.joining)

        // The following condition check if \uFFFD (replacement character for values unknown in Unicode) is not present in the lines read
        // If needed, to manage UTF8 BOM, we can add (lines.indexOf('\uFEFF') != 0) condition, i.e. \uFEFF (Byte Order Mark) is not present in position 0 of the file
        (lines.indexOf('\uFFFD') == -1)
      } catch {
        case _: UnsupportedEncodingException => false
      }
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_904,
      statusMessage = MOTIVAZIONE_CODIFICA_ERRATA
    )
  )

  /** Effettua l'unzip del file CSV contenuto nello zip [[zipMeta]].
   *
   *
   * All'interno di questa funzione vi sono un paio di controlli di ammissibilità sul file CSV.
   * Ad esempio se [[isRuleCheckNumberOfCsvFieldsEnabled]] è `true`, allora si controlla che il numero dei campi del file CSV sia quello atteso. */
  def unzipCsv(zipMeta: ZipRzg1Metadata, isRuleCheckNumberOfCsvFieldsEnabled: Boolean): ZipRzg1Metadata = {
    val zipFile = new ZipFile(zipMeta.file)
    val entries = zipFile.entries.asScala.toList.head
    //entry sarebbe un indice che mi dice a che punto sto nell'iterazione, quindi estraggo il file nello zip in base all'indice entry
    val csvInputStream = zipFile.getInputStream(entries)
    //operazione scopiazzata da internet, serve per crearti il tipo BufferedReader che ti serve per leggere riga per riga all'interno del csv
    val csvReader = new BufferedReader(new InputStreamReader(csvInputStream, "UTF-8"))
    val header = csvReader.readLine()
    val value = csvReader.readLine()
    if (header != null && header.nonEmpty && value != null && value.nonEmpty) {
      val csvFieldValue = value.split(CSV_DELIMITER)
      if (!isRuleCheckNumberOfCsvFieldsEnabled || value.count(_ == CSV_DELIMITER.charAt(0)) == Rzg1CsvSchema.getValues.length - 1)
        zipMeta.copy(csv = Some(
          CsvRzg1Metadata(
            fileName = entries.getName,
            header = header,
            data = getField(csvFieldValue, 0),
            id_indennizzo = getField(csvFieldValue, 1),
            piva_id = getField(csvFieldValue, 2),
            rag_soc_id = getField(csvFieldValue, 3),
            piva_udd = getField(csvFieldValue, 4),
            rag_soc_udd = getField(csvFieldValue, 5),
            om1_id = getField(csvFieldValue, 6),
            om2_id = getField(csvFieldValue, 7),
            om3_id = getField(csvFieldValue, 8)
          )))
      else {
        zipMeta.copy(
          isAmmissibile = false,
          statusCode = COD_904,
          statusMessage = MOTIVAZIONE_CSV_LINE
        )
      }
    } else {
      zipMeta.copy(
        isAmmissibile = false,
        statusCode = COD_904,
        statusMessage = MOTIVAZIONE_EMPTY_CSV
      )
    }
    //    var readLine: String = ""
    //    while ( {
    //      readline serve per leggere una riga del csv, ogni volta che richiami readLine legge la riga successiva
    //      readLine = csvReader.readLine()
    //      readLine != null
    //    }) {
    //      println(readLine)
    //    }
  }

  def getField(csvFields: Array[String], index: Int): Option[String] = {
    csvFields.lift(index).filter(_.nonEmpty)
  }

  /** Esegue sui file CSV estratti i controlli delle regole di ammissibilità attive. */
  def checkCsv(zipMeta: ZipRzg1Metadata, csvRules: List[CsvRule]): ZipRzg1Metadata = {
    /** Messaggio di ammissibilità positiva. */
    val okMessage = ReportMessage()
    val csv = zipMeta.csv.get
    /** Prima regola, se esiste e in ordine di priorità, che non viene rispettata dal file [[csv]]. */
    val errorRule = csvRules.find(rule => rule.isEnabled && !rule.condition(csv))
    /** Messaggio di errore da associare al file [[csv]]. */
    val message = if (errorRule.isDefined) errorRule.get.message else okMessage

    zipMeta.copy(
      isAmmissibile = message.isAmmissibile,
      statusCode = message.statusCode,
      statusMessage = message.statusMessage
    )
  }

  /** Controlla che il campo [[field]] sia non null. */
  def isNotNull(field: Option[String]): Boolean = field.nonEmpty && field.get.nonEmpty

  /** Controlla che il campo [[field]] sia di tipo data e nel formato [[DATA_COMP_FORMAT]]. */
  def isDate(field: Option[String]): Boolean = {
    val format = DateTimeFormat.forPattern(DATA_COMP_FORMAT)
    Try(format.parseDateTime(field.get)).isSuccess
  }

  /** Controlla che la lunghezza del campo [[field]] sia minore o uguale di [[length]]. */
  def checkStringLength(field: Option[String], length: Int): Boolean = isNotNull(field) && field.get.length <= length

  /** Controlla la correttezza dell'intestazione del CSV. */
  def ruleCheckHeader: CsvRule = CsvRule(
    ruleName = "ruleCheckHeader",
    isEnabled = Properties.isRuleCheckHeaderEnabled,
    condition = csv => {
      val header = csv.header
      val splitHeader = header.toUpperCase.split(CSV_DELIMITER)
      val schema = Rzg1CsvSchema.getValues.map(_.replace(_EURO_SYMBOL_, EURO).toUpperCase)

      (splitHeader.length == schema.length && schema.zipWithIndex.map({ case (value, index) => value.equals(Try(splitHeader(index)).getOrElse("")) }).min)
    },
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_904,
      statusMessage = MOTIVAZIONE_CSV_HEADER
    )
  )

  /** Controlla che il campo data all'interno del CSV sia nel formato previsto. */
  def ruleCheckDate: CsvRule = CsvRule(
    ruleName = "ruleCheckDate",
    isEnabled = Properties.isRuleCheckDateEnabled,
    condition = csv => isDate(csv.data),
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_904,
      statusMessage = MOTIVAZIONE_DATA)
  )

  /** Controlla che l'id indennizzo nel CSV abbia una lunghezza minore di 255 caratteri. */
  def ruleCheckIdIndennizzo: CsvRule = CsvRule(
    ruleName = "ruleCheckIdIndennizzo",
    isEnabled = Properties.isRuleCheckIdIndennizzoEnabled,
    condition = csv => checkStringLength(csv.id_indennizzo, 255),
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_904,
      statusMessage = MOTIVAZIONE_ID_INDENNIZZO
    )
  )

  /** Controlla che la piva id nel CSV abbia una lunghezza minore di 16 caratteri. */
  def ruleCheckPivaId: CsvRule = CsvRule(
    ruleName = "ruleCheckPivaId",
    isEnabled = Properties.isRuleCheckPivaIdEnabled,
    condition = csv => checkStringLength(csv.piva_id, 16),
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_904,
      statusMessage = MOTIVAZIONE_PIVA_ID
    )
  )

  /** Controlla che la piva udd nel CSV abbia una lunghezza minore di 16 caratteri. */
  def ruleCheckPivaUdd: CsvRule = CsvRule(
    ruleName = "ruleCheckPivaUdd",
    isEnabled = Properties.isRuleCheckPivaUddEnabled,
    condition = csv => checkStringLength(csv.piva_udd, 16),
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_904,
      statusMessage = MOTIVAZIONE_PIVA_UDD
    )
  )

  /** Controlla che la ragione sociale id nel CSV abbia una lunghezza minore di 255 caratteri. */
  def ruleCheckRagSocId: CsvRule = CsvRule(
    ruleName = "ruleCheckRagSocId",
    isEnabled = Properties.isRuleCheckRagSocIdEnabled,
    condition = csv => checkStringLength(csv.rag_soc_id, 255),
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_904,
      statusMessage = MOTIVAZIONE_RAGSOC_ID
    )
  )

  /** Controlla che la ragione sociale udd nel CSV abbia una lunghezza minore di 255 caratteri. */
  def ruleCheckRagSocUdd: CsvRule = CsvRule(
    ruleName = "ruleCheckRagSocUdd",
    isEnabled = Properties.isRuleCheckRagSocUddEnabled,
    condition = csv => checkStringLength(csv.rag_soc_udd, 255),
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_904,
      statusMessage = MOTIVAZIONE_RAGSOC_UDD
    )
  )

  /** Controlla che la piva id del CSV sia coerente con la piva contenuta nel nome del CSV. */
  def ruleCheckPivaIdConsistency: CsvRule = CsvRule(
    ruleName = "ruleCheckPivaIdConsistency",
    isEnabled = Properties.isRuleCheckPivaIdConsistencyEnabled,
    condition = csv => (csv.piva_id.getOrElse("") == csv.fileName.split("_").head),
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_904,
      statusMessage = MOTIVAZIONE_PIVA_ID_NON_COERENTE
    )
  )

  /** Controlla che la piva udd del CSV sia coerente con la piva contenuta nel nome del CSV. */
  def ruleCheckPivaUddConsistency: CsvRule = CsvRule(
    ruleName = "ruleCheckPivaUddConsistency",
    isEnabled = Properties.isRuleCheckPivaUddConsistencyEnabled,
    condition = csv => (csv.piva_udd.getOrElse("") == csv.fileName.split("_").apply(1)),
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_904,
      statusMessage = MOTIVAZIONE_PIVA_UDD_NON_COERENTE
    )
  )
}