package it.eng.au.portaleConsumi.dao.hive.misuregas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.misuregas.FornitureProcessiGasModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.misuregas.FornitureProcessiGasSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

class FornitureProcessiGasDao() extends HiveDao[FornitureProcessiGasModel] {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)
  override val tableName: String = Environment.getProperty("hive.table.forniture_processi_gas")
  override val schema: SchemaEnum = FornitureProcessiGasSchema

  def write(data: Dataset[FornitureProcessiGasModel]): Unit = {
    //val buckets = Environment.getProperty("hive.table.forniture_processi_gas.buckets").toInt
    data.write
      .mode("OVERWRITE")
      //.bucketBy(buckets, FornitureProcessiGasSchema.codice_fiscale)
      .insertInto(tableName)
  }

  def leggiUltimoCalcolo(): Dataset[FornitureProcessiGasModel] = {
    val dataUltimoCalcolo = dateCalcolo().head
    read()
      .where(col(FornitureProcessiGasSchema.data_calcolo) === dataUltimoCalcolo)
  }

  /** *
   * Cancella partizioni piu' vecchie rispetto all'ultimo calcolo
   */
  def cancellaDatiPrecedentiUltimaEsecuzione(): Unit = {
    val spark = Environment.getSpark
    val partizioni = dateCalcolo()
    logger.warn(s"Partizioni calcoli precedenti: ${partizioni.mkString(",")}")
    if (partizioni.length == 1) {
      logger.warn("Nessuna partizione da cancellare")
      return
    }
    val comando = comandoCancellazionePartizioni(partizioni)
    logger.warn(s"Esecuzione comando Hive: $comando")
    spark.sql(comando)
  }

  def comandoCancellazionePartizioni(partizioni: List[String]): String = {
    val partizioniDaCancellare = partizioni.drop(1)
      .map(x => s"""PARTITION (${FornitureProcessiGasSchema.data_calcolo}="$x")""") //unico modo per interpolare quotes in stringa
    s"ALTER TABLE $tableName DROP ${partizioniDaCancellare.mkString(", ")}"
  }

  /** *
   * Lista date di calcolo in ordine decrescente presenti nelle partizioni della tabella
   */
  def dateCalcolo(): List[String] = {
    read()
      .select(FornitureProcessiGasSchema.data_calcolo)
      .distinct()
      .orderBy(col(FornitureProcessiGasSchema.data_calcolo).desc)
      .collect()
      .map(x => x.getAs[String](0))
      .toList
  }

  /** *
   * Calcola delta delle forniture tra l'ultimo e il penultimo calcolo confrontando i codici pdr e l'hash del record
   */
  def calcolaDelta(nuovoDs: Dataset[FornitureProcessiGasModel] = null,
                   precedenteDs: Dataset[FornitureProcessiGasModel] = null): Dataset[FornitureProcessiGasModel] = {
    val spark = Environment.getSpark
    import spark.implicits._
    val colCodiceFiscaleTmp = "_tmp_codice_fiscale"

    val datePartizioni = dateCalcolo()
    val ds = read()
    if (datePartizioni.length == 1) {
      if (nuovoDs == null) {
        return ds
      } else {
        return nuovoDs
      }
    }

    val ultimoCalcoloDs = if (nuovoDs == null) {
      val dataUltimoCalcolo = datePartizioni(0)
      ds.where(col(FornitureProcessiGasSchema.data_calcolo) === dataUltimoCalcolo).persist()
    } else {
      nuovoDs
    }

    val penultimoCalcoloDs = if (precedenteDs == null) {
      val dataPenultimoCalcolo = datePartizioni(1)
      ds.where(col(FornitureProcessiGasSchema.data_calcolo) === dataPenultimoCalcolo)
    } else {
      precedenteDs
    }

    // possibile rivedere con windows function
    val codiciFiscaliAggiornatiDf = ultimoCalcoloDs
      .join(penultimoCalcoloDs,
        ultimoCalcoloDs(FornitureProcessiGasSchema.hashcode) === penultimoCalcoloDs(FornitureProcessiGasSchema.hashcode)
          and ultimoCalcoloDs(FornitureProcessiGasSchema.codice_pdr) === penultimoCalcoloDs(FornitureProcessiGasSchema.codice_pdr)
        , "LEFT_ANTI")
      .select(ultimoCalcoloDs(FornitureProcessiGasSchema.codice_fiscale))
      .withColumnRenamed(FornitureProcessiGasSchema.codice_fiscale, colCodiceFiscaleTmp)
      .dropDuplicates()

    val result = ultimoCalcoloDs
      .join(codiciFiscaliAggiornatiDf,
        ultimoCalcoloDs(FornitureProcessiGasSchema.codice_fiscale) === codiciFiscaliAggiornatiDf(colCodiceFiscaleTmp),
        "INNER")
      .selectExpr(columns: _*)
      .as[FornitureProcessiGasModel]

    ultimoCalcoloDs.unpersist()

    result
  }
}

