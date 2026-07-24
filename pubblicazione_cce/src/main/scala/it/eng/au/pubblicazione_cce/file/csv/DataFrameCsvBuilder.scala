package it.eng.au.pubblicazione_cce.file.csv

import it.eng.au.pubblicazione_cce.model.file.FileModel
import it.eng.au.pubblicazione_cce.schema.file.FileSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

/*
Interfaccia per definire gli elementi necessari per generare un file CSV da un DataFrame.
Valorizzare le liste per il raggruppamento delle informazioni e le funzioni per il calcolo delle varie componenti.
La funzione computeCsvElements aggiunge al dataframe le colonne presenti in FileModel (filename, percorso, contenuto)
 */
trait DataFrameCsvBuilder extends Serializable {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  // Carattere separatore elementi riga
  protected val COLUMN_SEPARATOR: String = ";"
  // Carattere nuova linea
  protected val LINE_SEPARATOR: String = "\n"
  // Carattere separatore percorso file system
  protected val PATH_SEPARATOR: String = "/"
  // Estensione file
  protected val FILE_EXTENSION: String = ".csv"

  // Lista nomi colonne file CSV
  val headerCsv: Option[List[String]] = None
  // Lista nomi colonne DataFrame da utilizzare per raggruppare elementi da inserire nello stesso file di output
  val columnsFileGroup: List[String]
  // Lista nomi colonne DataFrame da concatenare per creare le righe del file di output
  val columnsFileRowContent: List[String]
  // Numero massimo di righe per CSV.
  // se 0 non dividere il file e non inserire progressivo su nome file
  // se > 0 allora divide in piu' file e aggiunge progressivo a nome file
  val maxLineCsv: Int

  // Funzione per calcolare nome file (senza estensione) da colonne DataFrame
  // le colonne che si usano all'intero di questa funzione devono essere presenti nella lista columnsFileGroup
  def computeFileName: Column

  // Funzione per calcolare sotto cartelle da colonne DataFrame (deve finire con il carattere PATH_SEPARATOR)
  def computeSubDirectories: Column = lit(null)

  // Funzione per calcolare colonna del percorso base del file (root)
  def computePathRoot: Column

  // Seleziona da un dataframe le colonne per comporre un Dataset[FileModel] da utilizzare per la scrittura a file system
  def dfToFileModel(dataFrame: DataFrame): Dataset[FileModel] = dataFrame.selectExpr(FileSchema.getValues: _*).as[FileModel]

  /*
  Calcola elementi CSV aggiungendo al dataframe le informazioni per la generazione del file raggruppando le colonne
  per "columnsFileGroup" (colonne che determinano le righe che vanno nello stesso file).
  Le colonne aggiunte dalla funzione sono:
  - fileContent: contenuto del file, determinato dalle colonne "columnsFileRowContent", con header se specificato
  - filePathRoot: percorso root dove salvare i file (funzione: computePathRoot)
  - filePathSubDirectories: percorso sottocartelle dove salvare i file (funzione: computeSubDirectories)
  - fileName: nome file (funzione: computeFileName)
  - fileFullName: nome completo file pathRoot + pathSubDir + filename
  */
  def computeCsvElements(dataFrame: DataFrame): DataFrame = {
    // colonne per valori temporanei di calcolo
    val colRowContent = "tmp_row_content"
    // colonna progressivo, utilizzata solo se maxLineCsv > 0
    val colFileNumber = "tmp_file_number"
    val colRowNumber = "tmp_row_number"

    // variabili per processo
    // lista colonne raggruppamento (a cui verranno aggiunte eventuali colonne calcolate dal processo)
    var columnsFileGroupProcess = columnsFileGroup
    // dataframe finale
    var dfProcess = dataFrame

    // se previsto progressivo file
    if (maxLineCsv > 0) {
      val windowFileNumber = Window.partitionBy(columnsFileGroup.map(col): _*)
        .orderBy(columnsFileGroup.head) // ordinamento su primo elemento per non renderlo complicato per niente

      // calcola colonna progressivo
      dfProcess = dfProcess
        .withColumn(colRowNumber, row_number().over(windowFileNumber))
        .withColumn(colFileNumber, ((col(colRowNumber) - 1) / maxLineCsv).cast(IntegerType) + 1)

      // ed aggiungila a lista colonne raggruppamento file
      columnsFileGroupProcess = colFileNumber :: columnsFileGroupProcess
    }

    // converti elementi colonne contenuto in string, se null -> "" altrimenti file csv mancano delle colonne
    for(c <- columnsFileRowContent) {
      dfProcess = dfProcess
        .withColumn(c, coalesce(col(c), lit("")).cast(StringType))
    }

    dfProcess = dfProcess
      // calcola riga contenuto e raggruppa per colonne raggruppamento file
      .withColumn(colRowContent, concat_ws(COLUMN_SEPARATOR, columnsFileRowContent.map(col): _*))
      // raggruppa per determinare i file prodotti
      .groupBy(columnsFileGroupProcess.map(col): _*)
      // lista delle righe -> contenuto file finale
      .agg(collect_list(colRowContent).as(FileSchema.fileContent))

    // se header allora inserisci in testa a contenuto
    headerCsv match {
      case Some(header) => {
        // header e contenuto devono avere stesso numero colonne
        assert(header.length == columnsFileRowContent.length, "Number of column in row content do not match with header element count")

        dfProcess = dfProcess
          .withColumn(FileSchema.fileContent, concat_ws(LINE_SEPARATOR, lit(header.mkString(COLUMN_SEPARATOR)), col(FileSchema.fileContent)))
      }
      case None => {}
    }

    dfProcess = dfProcess
      // calcola percorsi e filename
      .withColumn(FileSchema.filePathRoot, computePathRoot)
      .withColumn(FileSchema.filePathSubDirectories, computeSubDirectories)
      .withColumn(FileSchema.fileName, computeFileName)

    if (maxLineCsv > 0) {
      // se si dividono i CSV sul numero di righe, aggiungi progressivo al nome file
      dfProcess = dfProcess.withColumn(FileSchema.fileName, concat(col(FileSchema.fileName), lit("_"), col(colFileNumber)))
    }

    dfProcess
      // aggiungi estensione
      .withColumn(FileSchema.fileName, concat(col(FileSchema.fileName), lit(FILE_EXTENSION)))
      // calcola full name
      .withColumn(FileSchema.fileFullName,
        concat(col(FileSchema.filePathRoot), col(FileSchema.filePathSubDirectories), col(FileSchema.fileName)))
      // seleziona solo colonne originali + colonne per file (FileSchema)
      .selectExpr((columnsFileGroup ++ FileSchema.getValues): _*)

  }

}
