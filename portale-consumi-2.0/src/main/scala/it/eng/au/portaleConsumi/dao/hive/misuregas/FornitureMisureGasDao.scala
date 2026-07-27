package it.eng.au.portaleConsumi.dao.hive.misuregas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.flow.misure.FornitureMisureGasArricchiteModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.flow.misure.FornitureMisureGasArricchiteSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, date_format}

class FornitureMisureGasDao() extends HiveDao[FornitureMisureGasArricchiteModel] {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)
  override val tableName: String = Environment.getProperty("hive.table.forniture_misure_gas")
  override val schema: SchemaEnum = FornitureMisureGasArricchiteSchema

  override def write(data: Dataset[FornitureMisureGasArricchiteModel], overwrite: Boolean): Unit = {
    super.write(data, overwrite=true)
  }

  /** *
   * Forniture con misure aggiornate sul periodo tra inizioAnnoMese e fineAnnoMese e con dataCalcolo maggiore uguale
   *
   * @param inizioAnnoMese nel formato yyyyMM
   * @param fineAnnoMese   nel formato yyyyMM
   * @param dataCalcolo    nel formato yyyy-MM-dd, indica la data di calcolo da cui iniziare a leggere le forniture aggiornate
   * @return
   */
  def fornitureDaAggiornare(inizioAnnoMese: String, fineAnnoMese: String, dataCalcolo: String): DataFrame = {
    read()
      .where(col(FornitureMisureGasArricchiteSchema.annomese) >= inizioAnnoMese)
      .where(col(FornitureMisureGasArricchiteSchema.annomese) <= fineAnnoMese)
      .where(date_format(col(FornitureMisureGasArricchiteSchema.data_caricamento), "yyyy-MM-dd") >= dataCalcolo)
      .select(col(FornitureMisureGasArricchiteSchema.codice_fornitura))
      .dropDuplicates()
  }

  def leggiMisure(inizioAnnoMese: String, fineAnnoMese: String, dataCalcolo: String = null): Dataset[FornitureMisureGasArricchiteModel] = {
    val ds = read()
      .where(col(FornitureMisureGasArricchiteSchema.annomese) >= inizioAnnoMese)
      .where(col(FornitureMisureGasArricchiteSchema.annomese) <= fineAnnoMese)
    dataCalcolo match {
      case null => ds
      case _ => ds.where(col(FornitureMisureGasArricchiteSchema.data_calcolo) >= dataCalcolo)
    }
  }

  /** *
   * Cancella partizioni obsolete con anno mese e ultima data calcolo inferiore a quanto passato
   */
  def cancellaDatiPrecedenti(ultimoAnnoMese: String, ultimaDataCalcolo: String): Unit = {
    val spark = Environment.getSpark

    val listaDateCalcoloDaCancellare = read()
      .where(col(FornitureMisureGasArricchiteSchema.data_calcolo) < ultimaDataCalcolo)
      .select(FornitureMisureGasArricchiteSchema.data_calcolo)
      .dropDuplicates()
      .collect()
      .map(x => x.getAs[String](0))
      .toList

    if (listaDateCalcoloDaCancellare.nonEmpty) {
      val comandoCancellazioneDateCalcolo = comandoCancellazionePartizioni(FornitureMisureGasArricchiteSchema.data_calcolo, listaDateCalcoloDaCancellare)
      logger.warn(s"Cancellazione partizioni per data calcolo")
      logger.warn(s"Esecuzione comando Hive: $comandoCancellazioneDateCalcolo")
      spark.sql(comandoCancellazioneDateCalcolo)
    } else {
      logger.warn(s"Nessuna partizione data_calcolo da cancellare")
    }

    val listaAnnoMeseDaCancellare = read()
      .where(col(FornitureMisureGasArricchiteSchema.annomese) < ultimoAnnoMese)
      .select(FornitureMisureGasArricchiteSchema.annomese)
      .dropDuplicates()
      .collect()
      .map(x => x.getAs[String](0))
      .toList

    if (listaAnnoMeseDaCancellare.nonEmpty) {
      val comandoCancellazioneAnnoMese = comandoCancellazionePartizioni(FornitureMisureGasArricchiteSchema.annomese, listaAnnoMeseDaCancellare)
      logger.warn(s"Cancellazione partizioni per anno mese")
      logger.warn(s"Esecuzione comando Hive: $comandoCancellazioneAnnoMese")
      spark.sql(comandoCancellazioneAnnoMese)
    } else {
      logger.warn(s"Nessuna partizione anno mese da cancellare")
    }
  }

  def comandoCancellazionePartizioni(nomePartizione: String, partizioni: List[String]): String = {
    val partizioniDaCancellare = partizioni.map(x => s"""PARTITION ($nomePartizione="$x")""") //unico modo per interpolare quotes in stringa
    s"ALTER TABLE $tableName DROP ${partizioniDaCancellare.mkString(", ")}"
  }

}
