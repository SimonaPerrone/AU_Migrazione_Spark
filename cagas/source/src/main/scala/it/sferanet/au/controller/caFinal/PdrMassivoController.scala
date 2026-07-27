package it.sferanet.au.controller.caFinal

import it.sferanet.au.schema._
import it.sferanet.au.utilities.Environment
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, LongType, StringType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

/**
 * Seconda parte ExportCa.sql senza CA
 * Ordine esecuzione: prima PDR massivo e poi CA Final
 */
class PdrMassivoController() {

  val tz: String = "yyyy-MM-dd"
  val dayMontYearPattern = "dd/MM/yyyy"

  /**
   *
   * @return Dataframe Dei Pdr Massivo (censimento tutti pdr) con delle informazioni aggiuntive
   *         per esempio zona climatica etc...
   */
  def get(): DataFrame = {

    getPdrMassivoFull
      .withColumn(PdrMassivoSchema.id_sag_ann, monotonically_increasing_id()) //unique id
      .withColumn(PdrMassivoSchema.anno_competenza, lit(Environment.getMassivoAnnoCompetenza).cast(StringType)) //constant from properties
      .withColumn(PdrMassivoSchema.prelievo_annuo_prev_forced, lit(null).cast(StringType)) //valued in Filter6
      .withColumn(PdrMassivoSchema.cod_prof_prel_std_forced, lit(null).cast(StringType)) //valued in Filter6
      .withColumn(PdrMassivoSchema.cat_uso_forced, lit(null).cast(StringType)) //valued in Filter6
      .withColumn(PdrMassivoSchema.zona_climatica_forced, lit(null).cast(StringType)) //valued in Filter6
      .withColumn(PdrMassivoSchema.classe_prelievo_forced, lit(null).cast(StringType)) //valued in Filter6
      .withColumn(PdrMassivoSchema.trattamento_forced, lit(null).cast(StringType)) //valued in Filter6
      .withColumn(PdrMassivoSchema.cap_trasp_pdr, lit(null).cast(StringType)) // not defined
      .withColumn(PdrMassivoSchema.tipo_trasmissione, lit(Environment.getTipoTrasmissione)) //constant from properties
      .withColumn(PdrMassivoSchema.calcmode, lit(null).cast(StringType))
      .select(selectExpression: _*)
  }

  def getRcuGasMassivoCaP: DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getRcugasMassivoPath)
  }

  def getRcuGasConnessioniDistr2P: DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getRcugasConnessioniDistr2Path)
  }

  def getRcuGasBilanciamentoP: DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getRcugasBilanciamentoPath)
  }

  def getPrtIstatRegioneClimaticaP: DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getIstatRegioneClimaticaPath)
  }

  def getRcugasVarTrattamentoP: DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getRcugasVarTrattamentoPath)
  }

  def getRcuGasVarPrelAnnuoP: DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getRcugasVarPrelAnnuoPath)
  }

  def getRcuGasVarProfiloP: DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getRcugasVarProfiloPath)
  }

  /**
   *
   * @param date     data da controllare
   * @param dateFrom limite superiore (se è null viene sostituita con 1900-01-01)
   * @param dateTo   limite inferiore (se è null viene sostituita con 2900-01-01)
   * @return ritorna un Column Boolean se @param date è contenuto tra dateFrom e dateTo
   */
  def betweenDates(date: Column, dateFrom: Column, dateTo: Column): Column =
    substring(coalesce(dateFrom, lit("1900-01-01")), 1, 10).cast(DateType) <= date.cast(DateType) and
      substring(coalesce(dateTo, lit("2900-01-01")), 1, 10).cast(DateType) >= date.cast(DateType)


  def selectExpression: List[Column] = {
    List(
      col(PdrMassivoSchema.id_sag_ann)
      , col(PdrMassivoSchema.anno_competenza)
      , col(PdrMassivoSchema.n_id_distr).cast(LongType)
      , col(PdrMassivoSchema.n_id_az_udd).cast(LongType)
      , col(PdrMassivoSchema.n_id_udb).cast(LongType)
      , col(RcuGasConnessioniDistr2PSchema.t_remi).as(PdrMassivoSchema.codice_remi)
      , col(RcuGasMassivoCaPSchema.t_codice_pdr).as(PdrMassivoSchema.codice_pdr)
      , col(PdrMassivoSchema.cap_trasp_pdr)
      , col(RcuGasMassivoCaPSchema.t_trattamento).as(PdrMassivoSchema.trattamento)
      , from_unixtime(unix_timestamp(), dayMontYearPattern).as(PdrMassivoSchema.d_ricezione)
      , col(PdrMassivoSchema.tipo_trasmissione)
      , col(PdrMassivoSchema.id_regione_climatica)
      //Columns to drop but used to join with CA
      , col(PdrMassivoSchema.t_cod_profilo)
      , col(PdrMassivoSchema.t_regione_climatica)
      , col(PdrMassivoSchema.n_prelievo_annuo)
      , col(PdrMassivoSchema.prelievo_annuo_prev_forced)
      , col(PdrMassivoSchema.cod_prof_prel_std_forced)
      , col(PdrMassivoSchema.cat_uso_forced)
      , col(PdrMassivoSchema.zona_climatica_forced)
      , col(PdrMassivoSchema.classe_prelievo_forced)
      , col(PdrMassivoSchema.trattamento_forced)
      , col(PdrMassivoSchema.calcmode)
      , col(PdrMassivoSchema.freeze_date)
    )
  }

  def getPdrMassivoFull: DataFrame = {
    val endContractContinuity = Environment.getContractContuinityUpperBoundDate

    /** Se la tabella rcugas.massivo contiene la parola "freeze", allora è stata generata dalla procedura di freeze (vedere progetto freezer_pre_calcolo);
    * in questo caso, si legge soltanto la partizione di interesse. Altrimenti si legge la tabella per intero.
    */
    val getMassivo = if (Environment.getRcugasMassivoPath.contains("freeze")) getRcuGasMassivoCaP
      .filter(col(RcuGasMassivoCaPSchema.execution_id) === Environment.getMassivoExecutionId)
    else getRcuGasMassivoCaP.withColumn(RcuGasMassivoCaPSchema.freeze_date, lit(null).cast(TimestampType))

    val rcuGasMassivoP = getMassivo
      .select(RcuGasMassivoCaPSchema.t_codice_pdr
        , RcuGasMassivoCaPSchema.n_id_pdr
        , RcuGasMassivoCaPSchema.n_id_az_udd
        , RcuGasMassivoCaPSchema.d_data_inizio_for
        , RcuGasMassivoCaPSchema.data_fine_for
        , RcuGasMassivoCaPSchema.t_comune_istat_pdr
        , RcuGasMassivoCaPSchema.t_anno_termico
        , RcuGasMassivoCaPSchema.id_regione_climatica
        , RcuGasMassivoCaPSchema.freeze_date
      )
      .distinct()
      .repartition(col(RcuGasMassivoCaPSchema.t_codice_pdr))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val varTrattamento = getRcugasVarTrattamentoP.select(
      RcuGasVarTrattamentoPSchema.n_id_pdr
      , RcuGasVarTrattamentoPSchema.d_data_inizio
      , RcuGasVarTrattamentoPSchema.d_data_fine
      , RcuGasVarTrattamentoPSchema.t_trattamento_settlement
    )
      .withColumnRenamed(RcuGasVarTrattamentoPSchema.t_trattamento_settlement, RcuGasMassivoCaPSchema.t_trattamento)
      .where(betweenDates(lit(endContractContinuity), col(RcuGasVarTrattamentoPSchema.d_data_inizio), col(RcuGasVarTrattamentoPSchema.d_data_fine)))

    val varPrelAnnuo = getRcuGasVarPrelAnnuoP.select(
      RcuGasVarPrelAnnuoPSchema.n_id_pdr
      , RcuGasVarPrelAnnuoPSchema.d_data_inizio
      , RcuGasVarPrelAnnuoPSchema.d_data_fine
      , RcuGasVarPrelAnnuoPSchema.n_prelievo_annuo
    )
      .where(betweenDates(lit(endContractContinuity), col(RcuGasVarPrelAnnuoPSchema.d_data_inizio), col(RcuGasVarPrelAnnuoPSchema.d_data_fine)))

    val varProfilo = getRcuGasVarProfiloP
      .select(
        RcuGasVarProfiloPSchema.n_id_pdr
        , RcuGasVarProfiloPSchema.d_data_inizio
        , RcuGasVarProfiloPSchema.d_data_fine
        , RcuGasVarProfiloPSchema.t_cod_profilo
      )
      .where(betweenDates(lit(endContractContinuity), col(RcuGasVarProfiloPSchema.d_data_inizio), col(RcuGasVarProfiloPSchema.d_data_fine)))

    val caP = rcuGasMassivoP
      .where(betweenDates(lit(endContractContinuity), col(RcuGasMassivoCaPSchema.d_data_inizio_for), col(RcuGasMassivoCaPSchema.data_fine_for)))
      .select(RcuGasMassivoCaPSchema.t_codice_pdr
        , RcuGasMassivoCaPSchema.n_id_pdr
        , RcuGasMassivoCaPSchema.n_id_az_udd
        , RcuGasMassivoCaPSchema.id_regione_climatica
        , RcuGasMassivoCaPSchema.freeze_date
      )

    val rcuDist = getRcuGasConnessioniDistr2P.where(betweenDates(lit(endContractContinuity), col(RcuGasConnessioniDistr2PSchema.d_data_inizio_conn), col(RcuGasConnessioniDistr2PSchema.d_data_fine_conn)))
      .select(RcuGasConnessioniDistr2PSchema.t_codice_pdr
        , RcuGasConnessioniDistr2PSchema.t_remi
        , RcuGasConnessioniDistr2PSchema.n_id_distr
        , RcuGasConnessioniDistr2PSchema.n_id_pdr)

    val rcuBP = getRcuGasBilanciamentoP.where(betweenDates(lit(endContractContinuity), col(RcuGasBilanciamentoPSchema.d_data_inizio), col(RcuGasBilanciamentoPSchema.d_data_fine)))
      .select(RcuGasBilanciamentoPSchema.n_id_pdr, RcuGasBilanciamentoPSchema.n_id_udb)

    val codIsRankWindowSpec = Window
      .partitionBy(RcuGasMassivoCaPSchema.t_codice_pdr)
      .orderBy(col(RcuGasMassivoCaPSchema.t_anno_termico).desc)
    val codIs = rcuGasMassivoP.withColumn(CodIsSchema.rank, rank.over(codIsRankWindowSpec))
      .select(CodIsSchema.t_codice_pdr
        , CodIsSchema.t_comune_istat_pdr
        , CodIsSchema.rank)

    val prtIstatRegioneClimaticaP = getPrtIstatRegioneClimaticaP

    val zonaClim = codIs
      .join(prtIstatRegioneClimaticaP, codIs(CodIsSchema.t_comune_istat_pdr) === prtIstatRegioneClimaticaP(PrtIstatRegioneClimaticaPSchema.t_codice_istat), "inner")
      .where(col(CodIsSchema.rank) === lit(1))
      .select(CodIsSchema.t_codice_pdr
        , CodIsSchema.t_comune_istat_pdr
        , PrtIstatRegioneClimaticaPSchema.t_regione_climatica)
      .distinct()

    val resultWithoutCa = caP
      // ADD TRATTAMENTO
      .join(varTrattamento, Seq(RcuGasMassivoCaPSchema.n_id_pdr.toString), "left")
      .drop(varTrattamento.col(RcuGasVarTrattamentoPSchema.n_id_pdr))
      .drop(varTrattamento.col(RcuGasVarTrattamentoPSchema.d_data_inizio))
      .drop(varTrattamento.col(RcuGasVarTrattamentoPSchema.d_data_fine))
      // ADD PREL_ANNUO
      .join(varPrelAnnuo, Seq(RcuGasMassivoCaPSchema.n_id_pdr.toString), "left")
      .drop(varTrattamento.col(RcuGasVarPrelAnnuoPSchema.n_id_pdr))
      .drop(varTrattamento.col(RcuGasVarPrelAnnuoPSchema.d_data_inizio))
      .drop(varTrattamento.col(RcuGasVarPrelAnnuoPSchema.d_data_fine))
      //ADD COD_PROF
      .join(varProfilo, Seq(RcuGasMassivoCaPSchema.n_id_pdr.toString), "left")
      .drop(varTrattamento.col(RcuGasVarProfiloPSchema.n_id_pdr))
      .drop(varTrattamento.col(RcuGasVarProfiloPSchema.d_data_inizio))
      .drop(varTrattamento.col(RcuGasVarProfiloPSchema.d_data_fine))
      //ADD COD_PROF
      .join(rcuDist, Seq(RcuGasMassivoCaPSchema.t_codice_pdr.toString), "inner")
      .drop(rcuDist(RcuGasConnessioniDistr2PSchema.n_id_pdr))

      .join(rcuBP, Seq(RcuGasConnessioniDistr2PSchema.n_id_pdr.toString), "left")
      .join(zonaClim, Seq(RcuGasMassivoCaPSchema.t_codice_pdr.toString), "left")

    resultWithoutCa
  }

}
