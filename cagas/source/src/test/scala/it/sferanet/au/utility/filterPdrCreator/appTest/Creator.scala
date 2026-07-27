package it.sferanet.au.utility.filterPdrCreator.appTest

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.filterPdr.Filter51EventOggettoVariazione
import it.sferanet.au.schema._
import it.sferanet.au.utilities.Environment
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}

import java.time.LocalDate

case class Creator() extends EnvironmentSparkTest with it.sferanet.au.utility.filterPdrCreator.Creator {

  override def createRcuGasMassivoP(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val pastJuly = new Filter51EventOggettoVariazione()
      .getPastJuly(LocalDate.now())
      .atTime(0, 0, 0, 0)

    List(
      ("150604000000000169", "05260000050547", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "C3E1", "2020", "10159", "Y", "", null, "u4"),
      ("150604000000000170", "05260000050570", "2030-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "C3E1", "2020", "10159", "Y", "", null, "u4"),
      ("150604000000000171", "05260000050571", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "C3E1", "2020", "10159", "Y", "", null, "u4"),
      ("150604000000000172", "05260000050572", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "C3E1", "2020", "10159", "Y", "", null, "u4"),
      ("150604000000000149", "05260000050549", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "C3E1", "2019", "10159", "Y", "", null, "u4"),
      ("150604000000000149", "05260000050549", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "C3E1", "2020", "10159", "Y", "", null, "u4"),
      ("1", "1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "C3E1", "2020", "10159", "Y", "", null, "u4"),
      ("2", "2", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "C3E1", "2020", "10159", "Y", "", null, "u4"),
      ("3", "3", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "C3E1", "2020", "10159", "Y", "", "not null", "u1"),
      ("4", "4", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "C3E1", "2020", "10159", "Y", "", "not null", "u1"),
      ("5.1", "5.1", s"$pastJuly", "2050-11-21 00:00:00.0", "016023", "C3E1", "2020", "10159", "Y", "", "not null", "u1"),
      ("5.2", "5.2", s"$pastJuly", "2050-11-21 00:00:00.0", "016023", "C3E1", "2020", "10159", "Y", "", "not null", "u1"),
      ("5.3", "5.3", s"$pastJuly", "2050-11-21 00:00:00.0", "016023", "C3E1", "2020", "10159", "Y", "", "not null", "u1")

    ).toDF(
      RcuGasMassivoCaPSchema.n_id_pdr,
      RcuGasMassivoCaPSchema.t_codice_pdr,
      RcuGasMassivoCaPSchema.d_data_inizio_for,
      RcuGasMassivoCaPSchema.data_fine_for,
      RcuGasMassivoCaPSchema.t_comune_istat_pdr,
      RcuGasMassivoCaPSchema.t_cod_profilo,
      RcuGasMassivoCaPSchema.t_anno_termico,
      RcuGasMassivoCaPSchema.n_prelievo_annuo,
      RcuGasMassivoCaPSchema.n_id_az_udd,
      RcuGasMassivoCaPSchema.t_trattamento,
      RcuGasMassivoCaPSchema.n_id_fornitura,
      RcuGasMassivoCaPSchema.piva_udd
    ).write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasMassivoPPath)
  }

  override def createRcuGasConnessioniDistr2P(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    List(
      ("05260000050547", "150604000000000169", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("05260000050548", "150604000000000148", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("05260000050549", "150604000000000149", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("05260000050570", "150604000000000170", "", "2030-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("05260000050571", "150604000000000171", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("05260000050572", "150604000000000172", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("1", "1", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("2", "2", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "d1", "", "", "", "1"),
      ("3", "3", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "d1", "", "", "", "1"),
      ("4", "4", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "d1", "", "", "", "1"),
      ("5.1", "5.1", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "d1", "", "", "", "1"),
      ("5.2", "5.2", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "d1", "", "", "", "1"),
      ("5.3", "5.3", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "d1", "", "", "", "1")


    ).toDF(RcuGasConnessioniDistr2PSchema.getValues: _*)
      .write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasConnessioniDistr2Path)
  }

  override def createRcuGasBilanciamentoP(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    List(
      ("aaa", "", "150604000000000169", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", ""),
      ("aaa", "", "1", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", ""),
      ("aaa", "id_udb_1", "4", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "")
    ).toDF(RcuGasBilanciamentoPSchema.getValues: _*)
      .write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasBilanciamentoPath)
  }

  override def createPrtIstatRegioneClimaticaP(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    List(
      ("016024", "A"),
      ("016025", "B"),
      ("016026", "C"),
      ("016027", "D"),
      ("016028", "E")
    ).toDF(PrtIstatRegioneClimaticaPSchema.getValues: _*)
      .write.mode(SaveMode.Overwrite).parquet(Environment.getIstatRegioneClimaticaPath)
  }

  override def createVRcuGasDistributoreP(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    List(("p1", "d1"))
      .toDF(
        VRcuGasDistributorePSchema.t_piva,
        VRcuGasDistributorePSchema.n_id_distributore)
      .write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasDistributorePath)
  }

  override def createRcuAziendaP(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    List(
      ("piva_udb_1", "id_azienda_1")
    ).toDF(
      RcuAziendaPSchema.t_piva,
      RcuAziendaPSchema.n_id_azienda
    ).write.mode(SaveMode.Overwrite).parquet(Environment.getRcuAziendaPath)
  }

  override def createRcuGasUdbP(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    List(
      ("id_udb_1", "id_azienda_1")
    ).toDF(
      RcuGasUdbPSchema.n_id_udb,
      RcuGasUdbPSchema.n_id_azienda
    ).write.mode(SaveMode.Overwrite).parquet(Environment.getRcuGasUdbPath)
  }

  override def createSettleGasGasTds(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    List(
      ("29/08/20 16:09:1580310593", "5.2", true)
    ).toDF(
      SettleGasGasTdsSchema.data_creazione,
      SettleGasGasTdsSchema.cod_pdr,
      SettleGasGasTdsSchema.valid
    ).write.mode(SaveMode.Overwrite).parquet(Environment.getGasTdsPath)
  }

  override def createCaFinal(): Unit = {
    val row = Row(10: Long, "2021", null, null, null, null, "5.3", "C3", "1", "E", null, "C3E1", "10159.0", null
      , "2021-02-04", "PRE", null, null, "1612430890541")

    val df = Environment.getSqlContext.createDataFrame(Environment.getSparkContext.parallelize(List(row)), StructType(
      StructField(CaFinalSchema.id_sag_ann, LongType) ::
        StructField(CaFinalSchema.anno_competenza, StringType) ::
        StructField(CaFinalSchema.n_id_distr, LongType) ::
        StructField(CaFinalSchema.n_id_az_udd, LongType) ::
        StructField(CaFinalSchema.n_id_udb, LongType) ::
        StructField(CaFinalSchema.codice_remi, StringType) ::
        StructField(CaFinalSchema.codice_pdr, StringType) ::
        StructField(CaFinalSchema.cap_trasp_pdr, StringType) ::
        StructField(CaFinalSchema.cat_uso, StringType) ::
        StructField(CaFinalSchema.classe_prelievo, StringType) ::
        StructField(CaFinalSchema.zona_climatica, StringType) ::
        StructField(CaFinalSchema.id_reg_clim, StringType) ::
        StructField(CaFinalSchema.cod_prof_prel_std, StringType) ::
        StructField(CaFinalSchema.prelievo_annuo_prev, StringType) ::
        StructField(CaFinalSchema.trattamento, StringType) ::
        StructField(CaFinalSchema.d_ricezione, StringType) ::
        StructField(CaFinalSchema.tipo_trasmissione, StringType) ::
        StructField(CaFinalSchema.codIstat, StringType) ::
        StructField(CaFinalSchema.executionid, StringType) ::
        Nil))
      .withColumn("pres_tds", lit(true))
      .withColumn("massivo_freeze_executionid", lit("1234567891234"))
      .selectExpr(CaFinalSchema.getValues: _*)
    df.show()
    df.printSchema()

    df.write
      .partitionBy(CaFinalSchema.anno_competenza, CaFinalSchema.executionid)
      .mode(SaveMode.Overwrite)
      .parquet(Environment.getCaFinalPath)
  }

  override def createCaPreFinal(): Unit = {
    val row = Row(10: Long, "2019", null, null, null, null, "5.3", "C3", "1", "E", null, "C3E1", "10159.0", null
      , "2021-02-04", "PRE", null, null, null, null, "dedotto", null, null, null, null, null, null, null, null, null
      , null, null, null, null, "true", null, "1612430890541")

    val df = Environment.getSqlContext.createDataFrame(Environment.getSparkContext.parallelize(List(row)), StructType(
      StructField(CaPreFinalSchema.id_sag_ann, LongType) ::
        StructField(CaPreFinalSchema.anno_competenza, StringType) ::
        StructField(CaPreFinalSchema.n_id_distr, LongType) ::
        StructField(CaPreFinalSchema.n_id_az_udd, LongType) ::
        StructField(CaPreFinalSchema.n_id_udb, LongType) ::
        StructField(CaPreFinalSchema.codice_remi, StringType) ::
        StructField(CaPreFinalSchema.codice_pdr, StringType) ::
        StructField(CaPreFinalSchema.cap_trasp_pdr, StringType) ::
        StructField(CaPreFinalSchema.cat_uso, StringType) ::
        StructField(CaPreFinalSchema.classe_prelievo, StringType) ::
        StructField(CaPreFinalSchema.zona_climatica, StringType) ::
        StructField(CaPreFinalSchema.id_reg_clim, StringType) ::
        StructField(CaPreFinalSchema.cod_prof_prel_std, StringType) ::
        StructField(CaPreFinalSchema.prelievo_annuo_prev, StringType) ::
        StructField(CaPreFinalSchema.trattamento, StringType) ::
        StructField(CaPreFinalSchema.d_ricezione, StringType) ::
        StructField(CaPreFinalSchema.tipo_trasmissione, StringType) ::
        StructField(CaPreFinalSchema.codistat, StringType) ::
        StructField(CaPreFinalSchema.id_ca_error_code, IntegerType) ::
        StructField(CaPreFinalSchema.start_local_file, StringType) ::
        StructField(CaPreFinalSchema.end_local_file, StringType) ::
        StructField(CaPreFinalSchema.calcmode, StringType) ::
        StructField(CaPreFinalSchema.start_t_misuratore_integrato, StringType) ::
        StructField(CaPreFinalSchema.end_t_misuratore_integrato, StringType) ::
        StructField(CaPreFinalSchema.start_t_pre_conv, StringType) ::
        StructField(CaPreFinalSchema.end_t_pre_conv, StringType) ::
        StructField(CaPreFinalSchema.pres_tds, BooleanType) ::
        StructField(CaPreFinalSchema.tipologia_uso, BooleanType) ::
        StructField(CaPreFinalSchema.comp_termica, BooleanType) ::
        StructField(CaPreFinalSchema.cat_uso_tds, StringType) ::
        StructField(CaPreFinalSchema.classe_prelievo_tds, StringType) ::
        StructField(CaPreFinalSchema.cod_istat_last_rcu, StringType) ::
        StructField(CaPreFinalSchema.zona_climatica_lookup, StringType) ::
        StructField(CaPreFinalSchema.prelievo_annuo_prev_forced, StringType) ::
        StructField(CaPreFinalSchema.cod_prof_prel_std_forced, StringType) ::
        StructField(CaPreFinalSchema.is_ca_calculated, BooleanType) ::
        StructField(CaPreFinalSchema.executionid, StringType) ::
        Nil))
    df.withColumn("d_ricezione_tmp", col(CaPreFinalSchema.d_ricezione).cast(DateType))
      .drop(CaPreFinalSchema.d_ricezione)
      .withColumnRenamed("d_ricezione_tmp", CaPreFinalSchema.d_ricezione)
      .withColumn(CaPreFinalSchema.cat_uso_forced, lit(null))
      .withColumn(CaPreFinalSchema.zona_climatica_forced, lit(null))
      .withColumn(CaPreFinalSchema.classe_prelievo_forced, lit(null))
      .withColumn(CaPreFinalSchema.n_coeff_correzione, lit(null))
      .withColumn(CaPreFinalSchema.trattamento_forced, lit(null))
      .withColumn(CaPreFinalSchema.startSegment, lit(null).cast(TimestampType))
      .withColumn(CaPreFinalSchema.endSegment, lit(null).cast(TimestampType))
      .withColumn(CaPreFinalSchema.massivo_freeze_executionid, lit("1234567891234"))
      .selectExpr(CaPreFinalSchema.getValues: _*)
    df.show()
    df.printSchema()

    df.write
      .partitionBy(CaPreFinalSchema.anno_competenza, CaPreFinalSchema.executionid)
      .mode(SaveMode.Overwrite)
      .parquet(Environment.getCaPreFinalPath)
  }

  override def createMeasures(): Unit = None

  override def createRcuTech(): Unit = None

  override def createRcuGasMassivoCaP(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val pastAugust = new Filter51EventOggettoVariazione()
      .getPastJuly(LocalDate.now())
      .plusMonths(1)
      .atTime(0, 0, 0, 0)
    //println(s"pastAugust: $pastAugust")

    List(
      ("150604000000000169", "05260000050547", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "C3E1", "2020", "10159", "Y", "", null, "u4"),
      ("150604000000000170", "05260000050570", "2030-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "C3E1", "2020", "10159", "Y", "", null, "u4"),
      ("150604000000000171", "05260000050571", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "C3E1", "2020", "10159", "Y", "", null, "u4"),
      ("150604000000000172", "05260000050572", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "C3E1", "2020", "10159", "Y", "", null, "u4"),
      ("150604000000000149", "05260000050549", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "C3E1", "2019", "10159", "Y", "", null, "u4"),
      ("150604000000000149", "05260000050549", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "C3E1", "2020", "10159", "Y", "", null, "u4"),
      ("1", "1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "C3E1", "2020", "10159", "Y", "", null, "u4"),
      ("2", "2", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "C3E1", "2020", "10159", "Y", "", null, "u4"),
      ("3", "3", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "C3E1", "2020", "10159", "Y", "", "not null", "u1"),
      ("4", "4", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "C3E1", "2020", "10159", "Y", "", "not null", "u1"),
      ("5.1", "5.1", s"$pastAugust", "2050-11-21 00:00:00.0", "016023", "C3E1", "2020", "10159", "Y", "", "not null", "u1"),
      ("5.2", "5.2", s"$pastAugust", "2050-11-21 00:00:00.0", "016023", "C3E1", "2020", "10159", "Y", "", "not null", "u1"),
      ("5.3", "5.3", s"$pastAugust", "2050-11-21 00:00:00.0", "016023", "C3E1", "2020", "10159", "Y", "", "not null", "u1")

    ).toDF(
      RcuGasMassivoCaPSchema.n_id_pdr,
      RcuGasMassivoCaPSchema.t_codice_pdr,
      RcuGasMassivoCaPSchema.d_data_inizio_for,
      RcuGasMassivoCaPSchema.data_fine_for,
      RcuGasMassivoCaPSchema.t_comune_istat_pdr,
      RcuGasMassivoCaPSchema.t_cod_profilo,
      RcuGasMassivoCaPSchema.t_anno_termico,
      RcuGasMassivoCaPSchema.n_prelievo_annuo,
      RcuGasMassivoCaPSchema.n_id_az_udd,
      RcuGasMassivoCaPSchema.t_trattamento,
      RcuGasMassivoCaPSchema.n_id_fornitura,
      RcuGasMassivoCaPSchema.piva_udd
    ).withColumn(RcuGasMassivoCaPSchema.id_regione_climatica, lit("14"))
      .withColumn(RcuGasMassivoSchema.startDate, lit("01/01/2019"))
      .withColumn(RcuGasMassivoSchema.endDate, lit("01/01/2021"))
      .withColumn(RcuGasMassivoCaPSchema.t_processo, lit(""))
      .withColumn(RcuGasMassivoCaPSchema.t_cod_cat_uso, lit(""))
      .write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasMassivoPath)
  }

  override def createRcuGasVarProfilo(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val pastAugust = new Filter51EventOggettoVariazione()
      .getPastJuly(LocalDate.now())
      .plusMonths(1)
      .atTime(0, 0, 0, 0)
    //println(s"pastAugust: $pastAugust")

    List(
      ("150604000000000169", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("150604000000000170", "2030-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("150604000000000171", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("150604000000000172", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("150604000000000149", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("150604000000000149", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("2", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("3", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("4", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("5.1", s"$pastAugust", "2050-11-21 00:00:00.0"),
      ("5.2", s"$pastAugust", "2050-11-21 00:00:00.0"),
      ("5.3", s"$pastAugust", "2050-11-21 00:00:00.0")

    ).toDF(
      RcuGasVarProfiloPSchema.n_id_pdr,
      RcuGasVarProfiloPSchema.d_data_inizio,
      RcuGasVarProfiloPSchema.d_data_fine
    ).withColumn(RcuGasVarProfiloPSchema.t_cod_profilo, lit("C3E1"))
      .withColumn(RcuGasVarProfiloPSchema.n_id_var_profilo, lit(""))
      .withColumn(RcuGasVarProfiloPSchema.t_anno, lit("2021"))
      .withColumn(RcuGasVarProfiloPSchema.t_cod_cat_uso, lit(""))
      .withColumn(RcuGasVarProfiloPSchema.t_cod_classe_prelievo, lit(""))
      .write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasVarProfiloPath)
  }

  override def createRcuGasVarTrattamento(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val pastAugust = new Filter51EventOggettoVariazione()
      .getPastJuly(LocalDate.now())
      .plusMonths(1)
      .atTime(0, 0, 0, 0)
    //println(s"pastAugust: $pastAugust")

    List(
      ("150604000000000169", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("150604000000000170", "2030-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("150604000000000171", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("150604000000000172", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("150604000000000149", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("150604000000000149", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("2", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("3", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("4", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("5.1", s"$pastAugust", "2050-11-21 00:00:00.0"),
      ("5.2", s"$pastAugust", "2050-11-21 00:00:00.0"),
      ("5.3", s"$pastAugust", "2050-11-21 00:00:00.0")

    ).toDF(
      RcuGasVarTrattamentoPSchema.n_id_pdr,
      RcuGasVarTrattamentoPSchema.d_data_inizio,
      RcuGasVarTrattamentoPSchema.d_data_fine
    ).withColumn(RcuGasVarTrattamentoPSchema.t_trattamento_settlement, lit("Y"))
      .write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasVarTrattamentoPath)
  }

  override def createRcuGasVarPrelAnnuo(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val pastAugust = new Filter51EventOggettoVariazione()
      .getPastJuly(LocalDate.now())
      .plusMonths(1)
      .atTime(0, 0, 0, 0)
    //println(s"pastAugust: $pastAugust")

    List(
      ("150604000000000169", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("150604000000000170", "2030-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("150604000000000171", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("150604000000000172", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("150604000000000149", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("150604000000000149", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("2", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("3", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("4", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("5.1", s"$pastAugust", "2050-11-21 00:00:00.0"),
      ("5.2", s"$pastAugust", "2050-11-21 00:00:00.0"),
      ("5.3", s"$pastAugust", "2050-11-21 00:00:00.0")

    ).toDF(
      RcuGasVarPrelAnnuoPSchema.n_id_pdr,
      RcuGasVarPrelAnnuoPSchema.d_data_inizio,
      RcuGasVarPrelAnnuoPSchema.d_data_fine
    ).withColumn(RcuGasVarPrelAnnuoPSchema.n_prelievo_annuo, lit("10159"))
      .write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasVarPrelAnnuoPath)
  }
}
