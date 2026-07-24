package it.eng.au.calcoloIndennizzi.controller

import it.eng.au.calcoloIndennizzi.schema.cig.{PdrCountSchema, PdrGSchema, PdrGSettimoSchema, PdrTotaleSchema}
import it.eng.au.calcoloIndennizzi.schema.measure.TglSchema
import it.eng.au.calcoloIndennizzi.schema.rcu.RcuAziendaSchema
import it.eng.au.calcoloIndennizzi.schema.rcugas._
import it.eng.au.calcoloIndennizzi.utility.Properties
import it.eng.au.indennizziMisureGasCommon.schema.{AggregatoTotaleSchema, DettaglioPdrSchema}
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.Try

/** Implementa una serie di operazioni per ottenere il dataframe PDR_G. */
object PdrGController extends Serializable {
  val hasIndennizzo = "has_indennizzo"
  val fileName = "file_name"
  val cloudPath = "cloud_path"
  val pdrCount = "pdr_count"
  val pivaUddJoin = "piva_udd_join"
  val pivaDistrJoin = "piva_distr_join"
  val dataAttivazione = "data_attivazione"
  val daysCount = "days_count"
  val daysCountPerPdr = "days_count_per_pdr"

  /**
   * A partire dal dataframe [[pdrTotale]], estrae i PdR per cui è stato generato un indennizzo e crea il dataframe di dettaglio PdR (corrisponde alla tabella cig_dettaglio_pdr).
   * @param pdrTotale dataframe che contiene le informazioni su tutto il perimetro dei PdR
   * @return simile a [[pdrTotale]] ma soltanto per i PdR per cui è stato generato un indennizzo
   */
  def getDettaglioPdr(pdrTotale: DataFrame): DataFrame = {
    pdrTotale
      .where(hasIndennizzo)
      .withColumnRenamed(PdrTotaleSchema.piva_distr, DettaglioPdrSchema.piva_id)
      .withColumnRenamed(PdrTotaleSchema.rag_soc_distr, DettaglioPdrSchema.rag_soc_id)
      .withColumnRenamed(PdrTotaleSchema.codice_pdr, DettaglioPdrSchema.pdr)
      .withColumnRenamed(PdrTotaleSchema.local_file, DettaglioPdrSchema.nome_file)
      .withColumn(DettaglioPdrSchema.executionid, lit(Environment.executionId).cast(LongType))
      .withColumn(DettaglioPdrSchema.id_indennizzo, hash(col(DettaglioPdrSchema.piva_udd), col(DettaglioPdrSchema.piva_id), col(DettaglioPdrSchema.executionid)).cast(LongType) + Int.MaxValue)
      .selectExpr(DettaglioPdrSchema.getValues: _*)
  }

  /**
   * A partire dai dataframe [[pdrGSettimo]] e [[aggregatoTotale]], estrae le informazioni utili per la creazione del dataframe pdrTotale (corrisponde alla tabella cig_pdr_totale).
   * @param pdrGSettimo dataframe contenente sia le informazioni sui PdR che sulle TGL
   * @param aggregatoTotale dataframe contenente le informazioni sugli indennizzi
   * @return dataframe pdrTotale, che contiene le informazioni su tutto il perimetro dei PdR
   */
  def getPdrTotale(pdrGSettimo: DataFrame, aggregatoTotale: DataFrame): DataFrame = {
    def getFileName: UserDefinedFunction = udf((localFile: String) => {
      localFile match {
        case null => null
        case _ => localFile
          .split(",")
          .map(file => Try(file.split("/").last).toOption)
          .map(name => name.getOrElse(None))
          .mkString(",")
      }
    })

    def getCloudPath: UserDefinedFunction = udf((localFile: String) => {
      localFile match {
        case null => null
        case _ => localFile
          .split(",")
          .map(file => Try(file.split("/").dropRight(1).last).toOption)
          .map(path => path.getOrElse(None))
          .mkString(",")
      }
    })

    val aggregatoJoin = aggregatoTotale
      .select(
        AggregatoTotaleSchema.piva_udd,
        AggregatoTotaleSchema.piva_distr,
        AggregatoTotaleSchema.id_indennizzo,
        AggregatoTotaleSchema.indennizzo_om1,
        AggregatoTotaleSchema.indennizzo_om2,
        AggregatoTotaleSchema.indennizzo_om3,
        AggregatoTotaleSchema.executionid
      )
      .withColumnRenamed(AggregatoTotaleSchema.piva_udd, pivaUddJoin)
      .withColumnRenamed(AggregatoTotaleSchema.piva_distr, pivaDistrJoin)

    pdrGSettimo
      .join(aggregatoJoin, pdrGSettimo(PdrGSettimoSchema.piva_udd) === aggregatoJoin(pivaUddJoin) && pdrGSettimo(PdrGSettimoSchema.piva_distr) === aggregatoJoin(pivaDistrJoin), "left")
      .drop(aggregatoJoin(pivaUddJoin))
      .drop(aggregatoJoin(pivaDistrJoin))
      .withColumn(fileName, getFileName(col(PdrGSettimoSchema.local_file)))
      .withColumn(cloudPath, getCloudPath(col(PdrGSettimoSchema.local_file)))
      .withColumn(PdrTotaleSchema.nome_file_tgl_om1, when(col(PdrGSettimoSchema.local_file).isNotNull && col(AggregatoTotaleSchema.indennizzo_om1) > 0 && !col(PdrGSettimoSchema.is_pdrG_om1), col(fileName)).otherwise("no"))
      .withColumn(PdrTotaleSchema.nome_file_tgl_om2, when(col(PdrGSettimoSchema.local_file).isNotNull && col(AggregatoTotaleSchema.indennizzo_om2) > 0 && !col(PdrGSettimoSchema.is_pdrG_om2), col(fileName)).otherwise("no"))
      .withColumn(PdrTotaleSchema.nome_file_tgl_om3, when(col(PdrGSettimoSchema.local_file).isNotNull && col(AggregatoTotaleSchema.indennizzo_om3) > 0 && !col(PdrGSettimoSchema.is_pdrG_om3), col(fileName)).otherwise("no"))
      .withColumn(PdrTotaleSchema.cartella_cloud_tgl_om1, when(col(PdrTotaleSchema.nome_file_tgl_om1) =!= lit("no"), col(cloudPath)).otherwise("no"))
      .withColumn(PdrTotaleSchema.cartella_cloud_tgl_om2, when(col(PdrTotaleSchema.nome_file_tgl_om2) =!= lit("no"), col(cloudPath)).otherwise("no"))
      .withColumn(PdrTotaleSchema.cartella_cloud_tgl_om3, when(col(PdrTotaleSchema.nome_file_tgl_om3) =!= lit("no"), col(cloudPath)).otherwise("no"))
      .withColumn(PdrTotaleSchema.data_attivazione_pdr, col(PdrTotaleSchema.data_attivazione_pdr).cast(TimestampType))
      .withColumn(hasIndennizzo,
        col(AggregatoTotaleSchema.indennizzo_om1) > 0 or
          col(AggregatoTotaleSchema.indennizzo_om2) > 0 or
          col(AggregatoTotaleSchema.indennizzo_om3) > 0)
  }

  /**
   * Calcola il numero di PDR_G, PDR_G_OM1, PDR_G_OM2, PDR_G_OM3 per ogni coppia (pivadistr, pivaudd), dove:
   *  - PDR_G - numero di PdR che fanno parte del perimetro;
   *  - PDR_G_OM# - numero di PdR che soddisfano la regola #.
   * @param pdrGSettimo dataframe contenente sia le informazioni sui PdR che sulle TGL
   * @return dataframe contenente le numeriche PDR_G, PDR_G_OM1, PDR_G_OM2, PDR_G_OM3
   */
  def getPdRCount(pdrGSettimo: DataFrame): DataFrame = {
    pdrGSettimo
      .groupBy(col(PdrGSettimoSchema.piva_udd), col(PdrGSettimoSchema.rag_soc_udd), col(PdrGSettimoSchema.piva_distr), col(PdrGSettimoSchema.rag_soc_distr))
      .agg(sum(col(PdrGSettimoSchema.is_pdrG).cast(LongType)).alias(PdrCountSchema.pdr_g),
        sum(col(PdrGSettimoSchema.is_pdrG_om1).cast(LongType)).alias(PdrCountSchema.pdr_g_om1),
        sum(col(PdrGSettimoSchema.is_pdrG_om2).cast(LongType)).alias(PdrCountSchema.pdr_g_om2),
        sum(col(PdrGSettimoSchema.is_pdrG_om3).cast(LongType)).alias(PdrCountSchema.pdr_g_om3))
      .selectExpr(PdrCountSchema.getValues: _*)
  }

  /**
   * Estrae le partite iva dalle tabelle in cui sono contenute.
   * @param pdrG dataframe dei PdR nel perimetro
   * @param rcuAziendaForId dataframe contenente le partite iva dell'id
   * @param rcuAziendaForUdd dataframe contenente le partite iva dell'udd
   * @return [[pdrG]] in aggiunta alle partite ive id e udd
   */
  def getRagioneSociale(pdrG: DataFrame, rcuAziendaForId: DataFrame, rcuAziendaForUdd: DataFrame): DataFrame = {
    pdrG
      .join(rcuAziendaForUdd, pdrG(PdrGSchema.piva_udd) === rcuAziendaForUdd(RcuAziendaSchema.t_piva), "left")
      .withColumnRenamed(RcuAziendaSchema.t_rag_soc, PdrGSchema.rag_soc_udd)
      .drop(rcuAziendaForUdd(RcuAziendaSchema.t_piva))
      .join(rcuAziendaForId, pdrG(PdrGSchema.piva_distr) === rcuAziendaForId(RcuAziendaSchema.t_piva), "left")
      .withColumnRenamed(RcuAziendaSchema.t_rag_soc, PdrGSchema.rag_soc_distr)
      .drop(rcuAziendaForId(RcuAziendaSchema.t_piva))
      .selectExpr(PdrGSchema.getValues: _*)
  }

  /**
   * Ottiene il dataframe pdRG, ovvero il dataframe contenente i PdR nel perimetro e le relative informazioni (classe misuratore, piva_distr, trattamento, ...).
   * @param rcugasMassivo dataframe su rcugas in cui sono presenti le informazioni base sul PdR e sulla fornitura
   * @param rcugasVarMisuratore dataframe su rcugas in cui è presente la classe misuratore
   * @param rcugasVarTrattamento dataframe su rcugas in cui è presente il trattamento
   * @param rcugasSospensioni dataframe su rcugas in cui sono presenti i PdR sospesi e il relativo periodo di sospensione
   * @param rcugasConnessioniDistr2 dataframe su rcugas in cui sono presenti le informazioni sul distributore
   * @return dataframe pdrG, contenente solo e soltanto i PdR che fanno parte del perimetro considerato, e le relative informazioni.
   */
  def getPdrGDataFrame(rcugasMassivo: DataFrame,
                       rcugasVarMisuratore: DataFrame,
                       rcugasVarTrattamento: DataFrame,
                       rcugasSospensioni: DataFrame,
                       rcugasConnessioniDistr2: DataFrame): DataFrame = {
    val pivaWindow = Window.partitionBy(col(RcugasMassivoPSchema.piva_udd), col(RcugasConnessioniDistr2Schema.t_piva_distr))

    val rcugasMassivoActivePdRs = PdrGController.removeSospesi(rcugasMassivo, rcugasSospensioni)

    rcugasMassivoActivePdRs
      .join(rcugasVarMisuratore, Seq("n_id_pdr"))

      .join(rcugasVarTrattamento, Seq("n_id_pdr"))

      .join(rcugasConnessioniDistr2, Seq("t_codice_pdr"))

      .withColumn(pdrCount, count(col(RcugasMassivoPSchema.t_codice_pdr)).over(pivaWindow))
      .where(col(pdrCount) > lit(10))

      .withColumnRenamed(RcugasMassivoPSchema.t_codice_pdr, PdrGSchema.codice_pdr)
      .withColumnRenamed(RcugasMassivoPSchema.d_data_inizio_for, PdrGSchema.data_attivazione_pdr)
      .withColumnRenamed(RcugasVarMisuratoreSchema.t_classe_misuratore, PdrGSchema.classe_gdm)
      .withColumnRenamed(RcugasConnessioniDistr2Schema.t_piva_distr, PdrGSchema.piva_distr)
      .withColumn(PdrGSchema.annomese, lit(Properties.getYearMonth))
      .select(
        PdrGSchema.codice_pdr,
        PdrGSchema.piva_udd,
        PdrGSchema.piva_distr,
        PdrGSchema.classe_gdm,
        PdrGSchema.data_attivazione_pdr,
        PdrGSchema.annomese
      )
      .distinct()
  }

  /**
   * Rimuove dal perimetro dei PdR i PdR sospesi per almeno un giorno nel mese considerato.
   * @param rcugasMassivo dataframe contenente tutti i PdR
   * @param rcugasSospensioni dataframe contenente le informazioni sui PdR sospesi
   * @return [[rcugasMassivo]] senza i PdR sospesi nel mese di competenza
   */
  def removeSospesi(rcugasMassivo: DataFrame, rcugasSospensioni: DataFrame): DataFrame = {
    val pdrWindow = Window.partitionBy(col(RcugasMassivoPSchema.t_codice_pdr))

    rcugasMassivo
      .join(rcugasSospensioni, rcugasMassivo(RcugasMassivoPSchema.n_id_pdr) === rcugasSospensioni(RcugasSospensioniPSchema.n_id_pdr)
        && rcugasMassivo(RcugasMassivoPSchema.n_id_fornitura) === rcugasSospensioni(RcugasSospensioniPSchema.n_id_fornitura),
        "left")
      .withColumn(daysCount, datediff(col(RcugasSospensioniPSchema.d_data_revoca_sosp), col(RcugasSospensioniPSchema.d_data_inizio_sosp)) + 1)
      .withColumn(daysCountPerPdr, sum(col(daysCount)).over(pdrWindow))
      .where(col(daysCountPerPdr).isNull or col(daysCountPerPdr) === 0)
      .drop(rcugasSospensioni(RcugasSospensioniPSchema.n_id_pdr))
      .drop(rcugasSospensioni(RcugasSospensioniPSchema.n_id_fornitura))
      .drop(daysCount)
      .drop(daysCountPerPdr)
      .withColumn(RcugasMassivoPSchema.d_data_inizio_for, min(col(dataAttivazione)).over(pdrWindow))
      .select(
        col(RcugasMassivoPSchema.n_id_pdr),
        col(RcugasMassivoPSchema.t_codice_pdr),
        col(RcugasMassivoPSchema.piva_udd),
        col(RcugasMassivoPSchema.d_data_inizio_for))
      .distinct()
  }

  /**
   * Ottiene il dataframe pdrGSettimo, che contiene sia le informazioni sui PdR nel perimetro, sia le informazioni sulle relative TGL
   * @param pdrG dataframe dei PdR nel perimetro
   * @param tgl dataframe delle TGL ammissibili
   * @return unico dataframe contenente tutte le informazioni necessarie al successivo conteggio dei PdR
   */
  def getPdrGSettimoDataFrame(pdrG: DataFrame, tgl: DataFrame): DataFrame = {
    pdrG
      .join(tgl, pdrG(PdrGSchema.codice_pdr) === tgl(TglSchema.cod_pdr), "left")
      .withColumn(PdrGSettimoSchema.is_pdrG, lit(true))
      .withColumn(PdrGSettimoSchema.is_pdrG_om1, when(col(TglController.isTglOM1) === true, lit(true)).otherwise(lit(false)))
      .withColumn(PdrGSettimoSchema.is_pdrG_om2, when(col(TglController.isTglOM2) === true, lit(true)).otherwise(lit(false)))
      .withColumn(PdrGSettimoSchema.is_pdrG_om3, when(col(TglController.isTglOM3) === true, lit(true)).otherwise(lit(false)))
      .selectExpr(PdrGSettimoSchema.getValues: _*)
  }

  /** Forza a null i valori delle regole disabilitate. */
  def forceNulls(pdrTotale: DataFrame): DataFrame = {
    val isOM1Enabled = Properties.isOM1Enabled
    val isOM2Enabled = Properties.isOM2Enabled
    val isOM3Enabled = Properties.isOM3Enabled

    val pdrTotaleWithOM1Disabled = if (!isOM1Enabled)
      pdrTotale
        .withColumn(PdrTotaleSchema.nome_file_tgl_om1, lit(null).cast(StringType))
        .withColumn(PdrTotaleSchema.cartella_cloud_tgl_om1, lit(null).cast(StringType))
    else pdrTotale

    val pdrTotaleWithOM2Disabled = if (!isOM2Enabled)
      pdrTotaleWithOM1Disabled
        .withColumn(PdrTotaleSchema.nome_file_tgl_om2, lit(null).cast(StringType))
        .withColumn(PdrTotaleSchema.cartella_cloud_tgl_om2, lit(null).cast(StringType))
    else pdrTotaleWithOM1Disabled

    val pdrTotaleWithOM3Disabled = if (!isOM3Enabled)
      pdrTotaleWithOM2Disabled
        .withColumn(PdrTotaleSchema.nome_file_tgl_om3, lit(null).cast(StringType))
        .withColumn(PdrTotaleSchema.cartella_cloud_tgl_om3, lit(null).cast(StringType))
    else pdrTotaleWithOM2Disabled

    pdrTotaleWithOM3Disabled
      .selectExpr(PdrTotaleSchema.getValues: _*)
  }
}