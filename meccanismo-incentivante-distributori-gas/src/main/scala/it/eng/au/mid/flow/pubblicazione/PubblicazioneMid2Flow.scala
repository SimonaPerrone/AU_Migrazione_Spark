package it.eng.au.mid.flow.pubblicazione

import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.dao.file.zip.ZipWriterDao
import it.eng.au.mid.dao.hive.mid.{Mid2DettaglioDao, MidAggregatoreInfoDao}
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.flow.Flow
import it.eng.au.mid.model.file.pubblicazione.{FileModel, MidCsvModel, ZipCsvModel, ZipWriterModel}
import it.eng.au.mid.model.hive.mid.{Mid2DettaglioModel, MidAggregatoreInfoModel}
import it.eng.au.mid.schema.file.pubblicazione._
import it.eng.au.mid.schema.hive.mid.{Mid1DettaglioSchema, Mid2DettaglioSchema}
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.joda.time.LocalDateTime

import java.io.File
import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter


class PubblicazioneMid2Flow extends Flow {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)
  override val flowName: String = "Pubblicazione MID2"

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  // input
  val mid2DettaglioDao = new Mid2DettaglioDao

  //output
  val zipWriterDao = new ZipWriterDao
  val midAggregatoreInfoDao = new MidAggregatoreInfoDao

  // parametri job
  val executionIdMid2Dettaglio: Long = Environment.getProperty("job.param.mid2_executionid_dett").toLong
  val sessioneForzata: String = Environment.getProperty("job.param.mid2_session")
  val percorsoSalvataggio: String = Environment.getProperty("file.path.mid2")
  val maxRighePerCsv: Int = Environment.getProperty("job.param.mid2_split").toInt
  val cseaPartitaIva: String = Environment.getProperty("job.param.csea_piva")
  //
  val csvSeparatore: String = ";"

  // parametri environment
  val executionId: Long = Environment.executionId
  val dataCalcolo: LocalDate = Environment.processDate
  val fileTimestamp: String = LocalDateTime.now().toString("yyyyMMddHHmmss")


  override def run(): Unit = {
    logger.warn(s"Inizio processo: $flowName")

    logger.warn(s"executionId: $executionId")
    logger.warn(s"dataCalcolo: $dataCalcolo")
    logger.warn(s"fileTimestamp: $fileTimestamp")
    logger.warn(s"executionIdMid2Dettaglio: $executionIdMid2Dettaglio")
    logger.warn(s"mid2_session: $sessioneForzata")
    logger.warn(s"maxRighePerCsv: $maxRighePerCsv")

    logger.warn(s"Lettura dati da pubblicare da ${mid2DettaglioDao.tableName}")
    val mid2Dettaglio = mid2DettaglioDao.read()
      .where(col(Mid1DettaglioSchema.executionid) === executionIdMid2Dettaglio)
    if (mid2Dettaglio.isEmpty) {
      throw new Exception(s"Nessun dato in ${mid2DettaglioDao.tableName} con ${Mid2DettaglioSchema.executionid} = $executionIdMid2Dettaglio")
    }

    val midValoriDs = calcolaRigheCsv(mid2Dettaglio, maxRighePerCsv)

    val csvInfoDs = definisciFileCsv(midValoriDs, fileTimestamp, sessioneForzata)

    val csvInfoZipFileDs = calcolaNomeFileZip(csvInfoDs, fileTimestamp, percorsoSalvataggio, sessioneForzata, dataCalcolo)

    val fileZipDs = definisciFileZip(csvInfoZipFileDs)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    logger.warn(s"Scrittura file Zip in percorso: $percorsoSalvataggio")
    zipWriterDao.write(fileZipDs)

    logger.warn(s"Scrittura tabella info: ${midAggregatoreInfoDao.tableName}")
    val dataCaricamento = Date.valueOf(dataCalcolo)
    val midAggregatoreInfo = calcolaAggregatoreInfo(fileZipDs, dataCaricamento, executionIdMid2Dettaglio, executionId, cseaPartitaIva)

    midAggregatoreInfoDao.write(midAggregatoreInfo)

    logger.warn(s"Fine processo: $flowName")
  }

  ///////


  /**
   * Calcola i valori per il file CSV e se necessario spezzare in piu' parti il CSV da produrre
   */
  def calcolaRigheCsv(mid2Dettaglio: Dataset[Mid2DettaglioModel], maxRighePerCsv: Int): Dataset[MidCsvModel] = {
    val rowNumberCol = "tmp_rn"

    // colonne per raggruppare i dati nei csv (eventualmente da dividere in piu file con piu progressivi)
    val windowCsv = Window.partitionBy(
      Mid2DettaglioSchema.piva_id
    ).orderBy(Mid2DettaglioSchema.piva_id) // serve un ordinamento per row_number

    val csvCols = Mid2Schema.getValues.map { c =>
      coalesce(col(c).cast("string"), lit(""))
    }

    mid2Dettaglio
      // rinomina colonne con nomi finali CSV
      .withColumn(Mid2Schema.PIVA_DISTR, col(Mid2DettaglioSchema.piva_id))
      .withColumn(Mid2Schema.RAGIONE_SOCIALE_DISTR, col(Mid2DettaglioSchema.rag_soc_id))
      .withColumn(Mid2Schema.STATO_DISTR, col(Mid2DettaglioSchema.stato_id))
      .withColumn(Mid2Schema.PIVA_UDD, col(Mid2DettaglioSchema.piva_udd))
      .withColumn(Mid2Schema.RAGIONE_SOCIALE_UDD, col(Mid2DettaglioSchema.rag_soc_udd))
      .withColumn(Mid2Schema.PIVA_DISTR_ATT, col(Mid2DettaglioSchema.piva_distr_att))
      .withColumn(Mid2Schema.RAGIONE_SOCIALE_DISTR_ATT, col(Mid2DettaglioSchema.rag_soc_distr_att))
      .withColumn(Mid2Schema.PDR, col(Mid2DettaglioSchema.pdr))
      .withColumn(Mid2Schema.ANNOMESE, col(Mid2DettaglioSchema.annomese))
      .withColumn(Mid2Schema.N, col(Mid2DettaglioSchema.contatore))
      .withColumn(Mid2Schema.COD_REMI, col(Mid2DettaglioSchema.cod_remi))
      .withColumn(Mid2Schema.GDM, col(Mid2DettaglioSchema.gdm))
      .withColumn(Mid2Schema.ALPHA, col(Mid2DettaglioSchema.alpha))
      // composizione riga csv
      //.withColumn(MidCsvSchema.riga_file, concat_ws(csvSeparatore, Mid2Schema.getValues.map(x => col(x)): _*))
      .withColumn(MidCsvSchema.riga_file, concat_ws(csvSeparatore, csvCols: _*))
      // aggiunta progressivo per nome file
      .withColumn(rowNumberCol, row_number().over(windowCsv))
      .withColumn(MidCsvSchema.progressivo_file, ((col(rowNumberCol) - 1) / maxRighePerCsv).cast(IntegerType) + 1)
      .withColumnRenamed(Mid2Schema.PIVA_DISTR, MidCsvSchema.piva_distr)
      .selectExpr(MidCsvSchema.getValues: _*)
      .as[MidCsvModel]
  }


  /** *
   * Aggrega dati csv e per ogni file definisce l'array di byte di cui è composto e il file name.
   * Ogni riga del dataset finale rappresenta un csv
   */
  def definisciFileCsv(midRigheCsv: Dataset[MidCsvModel], fileTimestamp: String, sessioneForzata: String): Dataset[ZipCsvModel] = {
    val contenutoCol = "tmp_lista_contenuto"
    val contenutoArrayCol = "tmp_array_contenuto"

    val intestazioneCsv = Mid2Schema.getValues.mkString(csvSeparatore)

    midRigheCsv
      // raggruppa per ottenere contenuto file
      .groupBy(
        col(MidCsvSchema.piva_distr),
        col(MidCsvSchema.progressivo_file)
      )
      .agg(collect_list(MidCsvSchema.riga_file).as(contenutoCol))
      // nome csv: <PIVA_ID>_MID2_<SESSIONE_FORZATA>_<timestamp>_<progressivo>.csv
      .withColumn(FileSchema.fileName,
        // concat nome file ed estensione
        concat(
          // concat_ws elementi nome file
          concat_ws("_",
            col(MidCsvSchema.piva_distr),
            lit(CostantiMid.NOME_OPERAZIONE_MID2),
            lit(sessioneForzata),
            lit(fileTimestamp),
            col(MidCsvSchema.progressivo_file)
          ),
          lit(".csv")
        )
      )
      // calcolo contenuto con intestazione
      .withColumn(contenutoArrayCol, array_union(lit(Array(intestazioneCsv)), col(contenutoCol)))
      .withColumn(FileSchema.content, concat_ws("\n", col(contenutoArrayCol)))
      // trasformazione in dato finale
      .map(r => ZipCsvModel(
        pivaId = r.getAs[String](MidCsvSchema.piva_distr),
        zipFileName = null,
        // dati CSV
        fileModel = FileModel(
          fileName = r.getAs[String](FileSchema.fileName),
          content = r.getAs[String](FileSchema.content).getBytes
        )
      ))
  }

  /**
   * Calcola percorso e nome file zip
   *
   * @param ds              dataset
   * @param fileTimestamp   timestamp da riportare nel nome file
   * @param percorsoFile    percorso base dei file senza "/" finale
   * @param sessioneForzata stringa che rappresenta la sessione che deve essere scritta nel nome file
   */
  def calcolaNomeFileZip(ds: Dataset[ZipCsvModel], fileTimestamp: String, percorsoFile: String, sessioneForzata: String,
                         dataCalcolo: LocalDate): Dataset[ZipCsvModel] = {
    val progressivoCol = "tmp_progessivo"
    val percorsoFileCol = "tmp_percorso_file"
    val annoCalcolo = dataCalcolo.getYear.toString
    val meseCalcolo = dataCalcolo.format(DateTimeFormatter.ofPattern("MM"))

    ds
      // calcolo zip file name
      // progressivo sempre = 1; non e' attesa la necessita' di spezzare gli zip
      .withColumn(progressivoCol, lit("1"))
      // percorsoSalvataggio/ANNO/MESE/
      .withColumn(percorsoFileCol, concat(
        lit(percorsoFile),
        lit("/"),
        lit(annoCalcolo),
        lit("/"),
        lit(meseCalcolo),
        lit("/")
      ))
      .withColumn(ZipCsvSchema.zipFileName,
        // percorso + nome file + estensione
        concat(
          // percorso
          col(percorsoFileCol),
          // nome file
          concat_ws("_",
            col(ZipCsvSchema.pivaId),
            lit(CostantiMid.NOME_OPERAZIONE_MID2),
            lit(sessioneForzata),
            lit(fileTimestamp),
            col(progressivoCol)
          ),
          // estensione
          lit(".zip")
        )
      )
      .selectExpr(ZipCsvSchema.getValues: _*)
      .as[ZipCsvModel]
  }


  /**
   * Aggrega file csv in un unico file zip e calcola percorso di salvataggio
   */
  def definisciFileZip(zipCsv: Dataset[ZipCsvModel]): Dataset[ZipWriterModel] = {
    zipCsv
      .groupBy(
        col(ZipCsvSchema.zipFileName).as(ZipWriterSchema.fileName),
        col(ZipCsvSchema.pivaId))
      .agg(collect_list(ZipCsvSchema.fileModel).as(ZipWriterSchema.files))
      .selectExpr(ZipWriterSchema.getValues: _*)
      .as[ZipWriterModel]
  }

  /**
   * Calcola valori per completare tabella Aggregatore Info
   */
  def calcolaAggregatoreInfo(ds: Dataset[ZipWriterModel], dataCaricamento: Date, executionIdMidDettaglio: Long,
                             executionId: Long, cseaPartitaIva: String): Dataset[MidAggregatoreInfoModel] = {
    ds.map(r => MidAggregatoreInfoModel(
      operation_name = CostantiMid.NOME_OPERAZIONE_MID2,
      nome_file = new File(r.fileName).getName,
      path = r.fileName,
      tipo_dest = CostantiMid.DESTINAZIONE_MID2,
      piva_dest = cseaPartitaIva,
      piva_id_file = r.pivaId,
      piva_udd_file = null, // sempre a null
      data_caricamento = dataCaricamento,
      executionid_mid_dettaglio = executionIdMidDettaglio,
      executionid = executionId
    ))
  }

}
