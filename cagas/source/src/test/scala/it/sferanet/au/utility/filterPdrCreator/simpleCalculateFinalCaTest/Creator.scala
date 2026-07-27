package it.sferanet.au.utility.filterPdrCreator.simpleCalculateFinalCaTest

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.model.prestazionale.{Im1Post, Im1Pre}
import it.sferanet.au.model.rettifica.Rsl
import it.sferanet.au.schema._
import it.sferanet.au.utilities.Environment
import it.sferanet.au.utility.filterPdrCreator
import org.apache.spark.sql.functions.{col, concat, lit, substring}
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.text.SimpleDateFormat


case class Creator() extends EnvironmentSparkTest with filterPdrCreator.Creator {
  private lazy val format = new SimpleDateFormat("yyyy-mm-dd")

  /**
   *
   * @return il mock delle misure
   */
  def createMeasures(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val measuresRml: DataFrame = List(
      ("1", "RML", "04/01/2019", "1", "5.1", "52", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "1", "01-01-2019T00:00:00.000000"),
      ("1", "RML", "01/01/2020", "1", "100", "10", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "1", "01-01-2019T00:00:00.000000"),
      ("1", "RML", "02/01/2020", "1", "102", "20", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "1", "01-01-2019T00:00:00.000000"),
      ("1", "RML", "03/01/2020", "1", "103", "30", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "1", "01-01-2019T00:00:00.000000"),
      ("1", "RML", "04/01/2020", "1", "104", "40", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_10.xml", "1", "01-01-2019T00:00:00.000000"),
      ("1", "RML", "05/01/2020", "1", "105", "50", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151502_1.xml", "1", "01-01-2019T00:00:00.000000"),
      ("2", "RML", "04/01/2019", "1", "5.2", "52", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "1", "01-01-2019T00:00:00.000000"),
      ("2", "RML", "01/01/2020", "1", "102", "10", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "1", "01-01-2019T00:00:00.000000"),
      ("2", "RML", "02/01/2020", "1", "103", "20", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "1", "01-01-2019T00:00:00.000000"),
      ("2", "RML", "03/01/2020", "1", "104", "30", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "1", "01-01-2019T00:00:00.000000"),
      ("2", "RML", "04/01/2020", "1", "105", "40", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_10.xml", "1", "01-01-2019T00:00:00.000000"),
      ("2", "RML", "05/01/2020", "1", "106", "50", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151502_1.xml", "1", "01-01-2019T00:00:00.000000"),
      ("11", "RML", "04/01/2019", "1", "5.1", "52", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "1", "01-01-2019T00:00:00.000000"),
      ("11", "RML", "01/01/2020", "1", "100", "10", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "1", "01-01-2019T00:00:00.000000"),
      ("11", "RML", "02/01/2020", "1", "102", "20", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "1", "01-01-2019T00:00:00.000000")

    ).toDF(
      RmlSchema.cod_pdr, //cod_pdr - pdr
      RmlSchema.cod_servizio, //cod_servizio - servizio
      RmlSchema.data_racc, //date - dataRacc
      RmlSchema.mot_rett_lett, //mot_rett_lett -readType
      RmlSchema.let_tot_prel, //let_tot_prel -measure
      RmlSchema.let_tot_conv, //let_tot_conv -converted
      RmlSchema.matr_mis, //matr_mis -serialNumberMis
      RmlSchema.matr_conv, //matr_conv -serialNumberConv
      RmlSchema.local_file, //local_file
      RmlSchema.motRettLet, //mottRettLett
      RmlSchema.d_caricamento //d_caricamento
    )
    val measuresIm1Pre = Environment.getSparkContext.parallelize(List(
      Im1Pre(pdr = "3", service = "IM1Pre", date = Some(format.parse("2019-10-05")), readType = Some('E'), measure = Some(200),
        converted = None, serialNumberConv = Some("MTSB038602099035"), serialNumberMis = Some("MTSB038602099035"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_13171830154/DISTRIBUTORE/TMG_13171830154_02259960306/2020/0422/13171830154_02259960306_201910_IM10360_20200422113114_4.xml"),
        d_caricamento = Some(format.parse("2020-04-22")), coefCorr = Some(1), cau_int_mis = None, cau_int_cor = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(pdr = "3", service = "IM1Post", date = Some(format.parse("2019-10-05")), readType = None, measure = Some(201),
        converted = None, serialNumberConv = Some("MTSB038602099035"), serialNumberMis = Some("MTSB038602099035"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_13171830154/DISTRIBUTORE/TMG_13171830154_02259960306/2020/0428/13171830154_02259960306_201910_IM10306_20200428145346_1.xml"),
        d_caricamento = Some(format.parse("2020-04-28")), coefCorr = Some(1), cau_int_mis = None, cau_int_cor = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),

      Im1Post(pdr = "4", service = "IM1Post", date = Some(format.parse("2019-10-06")), readType = Some('E'), measure = Some(300),
        converted = None, serialNumberConv = Some("MTSB038602099035"), serialNumberMis = Some("MTSB038602099035"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_13171830154/DISTRIBUTORE/TMG_13171830154_02259960306/2020/0428/13171830154_02259960306_201910_IM10306_20200428145346_1.xml"),
        d_caricamento = Some(format.parse("2020-04-22")), coefCorr = Some(1), cau_int_mis = None, cau_int_cor = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Pre(pdr = "4", service = "IM1Pre", date = Some(format.parse("2019-10-06")), readType = Some('E'), measure = Some(301),
        converted = None, serialNumberConv = Some("MTSB038602099035"), serialNumberMis = Some("MTSB038602099035"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_13171830154/DISTRIBUTORE/TMG_13171830154_02259960306/2020/0428/13171830154_02259960306_201910_IM10306_20200428145346_1.xml"),
        d_caricamento = Some(format.parse("2020-04-28")), coefCorr = Some(1), cau_int_mis = None, cau_int_cor = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Im1Post(pdr = "4", service = "IM1Post", date = Some(format.parse("2019-10-06")), readType = Some('E'), measure = Some(302),
        converted = None, serialNumberConv = Some("MTSB038602099035"), serialNumberMis = Some("MTSB038602099035"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ/TMG_13171830154/DISTRIBUTORE/TMG_13171830154_02259960306/2020/0428/13171830154_02259960306_201910_IM10306_20200428145346_1.xml"),
        d_caricamento = Some(format.parse("2020-04-28")), coefCorr = Some(1), cau_int_mis = None, cau_int_cor = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)))
    val measuresRsl = Environment.getSparkContext.parallelize(List(
      Rsl(pdr = "5", service = "RSL", date = Some(format.parse("2020-02-05")), collected = Some("S"), measure = Some(5),
        converted = Some(50), serialNumberConv = Some("MTSB038602099035"), serialNumberMis = Some("MTSB038602099035"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ_RECUPERO/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12300020158/2019/1004/05608890488_12300020158_201909_RSL0400_20191004112405_1.xml"),
        motivation = Some(0), d_caricamento = Some(format.parse("2020-01-03")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rsl(pdr = "5", service = "RSL", date = Some(format.parse("2020-02-05")), collected = Some("S"), measure = Some(5.5),
        converted = Some(50), serialNumberConv = Some("MTSB038602099035"), serialNumberMis = Some("MTSB038602099035"),
        local_file = Some("/mnt/isilonshare1/GAS_INJ_RECUPERO/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_06655971007/2019/1004/05608890488_06655971007_201909_RSL0400_20191004112405_1.xml"),
        motivation = Some(0), d_caricamento = Some(format.parse("2020-01-03")), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)))


    measuresRml
      .withColumn("annomese", concat(substring(col(RmlSchema.data_racc.toString), 7, 4), substring(col(RmlSchema.data_racc.toString), 4, 2)))
      .write.partitionBy("annomese").mode(SaveMode.Overwrite).parquet(Environment.getRmlParquetPath)

  }

  def createRcuTech(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val df: DataFrame = List(
      ("01/01/2020", "02/01/2020", "1", "2", "", "NO", "0", "0"),
      ("02/01/2020", "03/01/2020", "1", "2", "", "SI", "0", "0"),
      ("03/01/2020", "04/01/2020", "1", "2", "", "SI", "0", "0"),
      ("04/01/2020", "05/01/2020", "1", "2", "", "NO", "0", "0"),

      ("01/01/2020", "02/01/2020", "2", "2", "", "NO", "0", "0"),
      ("02/01/2020", "03/01/2020", "2", "2", "", "SI", "0", "0"),
      ("03/01/2020", "04/01/2020", "2", "2", "", "SI", "0", "0"),
      ("04/01/2020", "05/01/2020", "2", "2", "", "NO", "0", "0"),

      ("01/01/2020", "02/01/2020", "11", "2", "", "NO", "0", "0"),
      ("02/01/2020", "03/01/2020", "11", "2", "", "SI", "0", "0"),
      ("03/01/2020", "04/01/2020", "11", "2", "", "SI", "0", "0"),
      ("04/01/2020", "05/01/2020", "11", "2", "", "NO", "0", "0"))
      .toDF(RcuGasMassivoTechSchema.startDate,
        RcuGasMassivoTechSchema.endDate,
        RcuGasMassivoTechSchema.t_codice_pdr,
        RcuGasMassivoTechSchema.n_coeff_correzione, //n_coeff_correzione
        RcuGasMassivoTechSchema.t_misuratore_integrato,
        RcuGasMassivoTechSchema.t_pre_conv,
        RcuGasMassivoTechSchema.n_num_cifre_convertitore,
        RcuGasMassivoTechSchema.n_num_cifre_misuratore)
    df.write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasTechPath)


  }

  def createRcuGasMassivoCaP(): Unit = {
    //    rcugas.basepath
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    //    SparkTest.Environment.getSparkContext.parallelize(List(
    //      RcuGasMassivo(startDate = format.parse("2019-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "1", cat_uso = null, t_cod_profilo = "T2B1",
    //        t_processo = null, id_regione_climatica = 14, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null),
    //      RcuGasMassivo(startDate = format.parse("2019-01-01"), endDate = format.parse("2021-01-01"), t_codice_pdr = "2", cat_uso = null, t_cod_profilo = "T2B1",
    //        t_processo = null, id_regione_climatica = 14, t_comune_istat_pdr = null, t_anno_termico = 0, n_prelievo_annuo = null)
    //    ))
    val df: DataFrame = List(
      ("1", "1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "0", "Y", "", "14", "id_fornitura_1", "piva_udd_1"),
      ("2", "2", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "0", "Y", "", "14", "id_fornitura_2", "piva_udd_2"),
      ("3", "3", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "0", "Y", "", "14", "id_fornitura_3", "piva_udd_3"),
      ("4", "4", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "0", "Y", "", "14", "id_fornitura_4", "piva_udd_4"),
      ("5", "5", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "T2B1", "2019", "0", "Y", "", "14", "id_fornitura_5", "piva_udd_5"),
      ("6", "6", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "T2B1", "2020", "0", "Y", "", "14", "id_fornitura_6", "piva_udd_6"),
      ("7", "7", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "T2B1", "2020", "0", "Y", "", "14", "id_fornitura_7", "piva_udd_7"),
      ("11", "11", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "T2B1", "2020", "0", "Y", "", "14", "id_fornitura_7", "piva_udd_11"),
      ("RAF11", "RAF11", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "0", "Y", "", "14", "id_fornitura_1", "piva_udd_1"),
      ("RAF12", "RAF12", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "0", "Y", "", "14", "id_fornitura_1", "piva_udd_1")
    ).toDF(RcuGasMassivoCaPSchema.n_id_pdr,
      RcuGasMassivoCaPSchema.t_codice_pdr,
      RcuGasMassivoCaPSchema.d_data_inizio_for,
      RcuGasMassivoCaPSchema.data_fine_for,
      RcuGasMassivoCaPSchema.t_comune_istat_pdr,
      RcuGasMassivoCaPSchema.t_cod_profilo,
      RcuGasMassivoCaPSchema.t_anno_termico,
      RcuGasMassivoCaPSchema.n_prelievo_annuo,
      RcuGasMassivoCaPSchema.n_id_az_udd,
      RcuGasMassivoCaPSchema.t_trattamento,
      RcuGasMassivoCaPSchema.id_regione_climatica,
      RcuGasMassivoCaPSchema.n_id_fornitura,
      RcuGasMassivoCaPSchema.piva_udd)
      .withColumn(RcuGasMassivoSchema.startDate, lit("01/01/2019"))
      .withColumn(RcuGasMassivoSchema.endDate, lit("01/01/2021"))
      .withColumn(RcuGasMassivoCaPSchema.t_processo, lit(""))
      .withColumn(RcuGasMassivoCaPSchema.t_cod_cat_uso, lit(""))
    df.write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasMassivoPath)
  }

  override def createRcuGasVarProfilo(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("2", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("3", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("4", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("5", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("6", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("7", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("8", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("9", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("10", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("11", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("RAF11", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("RAF12", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0")
    ).toDF(
      RcuGasVarProfiloPSchema.n_id_pdr,
      RcuGasVarProfiloPSchema.d_data_inizio,
      RcuGasVarProfiloPSchema.d_data_fine
    ).withColumn(RcuGasVarProfiloPSchema.t_cod_profilo, lit("T2B1"))
      .withColumn(RcuGasVarProfiloPSchema.n_id_var_profilo, lit(""))
      .withColumn(RcuGasVarProfiloPSchema.t_anno, lit("2021"))
      .withColumn(RcuGasVarProfiloPSchema.t_cod_cat_uso, lit(""))
      .withColumn(RcuGasVarProfiloPSchema.t_cod_classe_prelievo, lit(""))
      .write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasVarProfiloPath)
  }

  override def createRcuGasVarTrattamento(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("2", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("3", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("4", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("5", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("6", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("7", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("8", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("9", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("10", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("11", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("RAF11", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("RAF12", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0")
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

    List(
      ("1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("2", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("3", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("4", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("5", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("6", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("7", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("8", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("9", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("10", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("11", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("RAF11", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("RAF12", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0")
    ).toDF(
      RcuGasVarPrelAnnuoPSchema.n_id_pdr,
      RcuGasVarPrelAnnuoPSchema.d_data_inizio,
      RcuGasVarPrelAnnuoPSchema.d_data_fine
    ).withColumn(RcuGasVarPrelAnnuoPSchema.n_prelievo_annuo, lit("10159"))
      .write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasVarPrelAnnuoPath)
  }

  def createRcuGasMassivoP(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", "1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "10159", "Y", "", "14"),
      ("2", "2", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "10159", "Y", "", "14"),
      ("3", "3", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "10159", "Y", "", ""),
      ("4", "4", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "10159", "Y", "", ""),
      ("5", "5", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "T2B1", "2019", "10159", "Y", "", ""),
      ("6", "6", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "T2B1", "2020", "10159", "Y", "", ""),
      ("7", "7", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "T2B1", "2020", "10159", "Y", "", ""),
      ("11", "11", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016025", "C2B1", "2020", "10159", "Y", "", "14")


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
      RcuGasMassivoCaPSchema.id_regione_climatica
    ).write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasMassivoPPath)
  }

  def createRcuGasConnessioniDistr2P(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    List(
      ("1", "1", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("2", "2", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("3", "3", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("4", "4", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("5", "5", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("6", "6", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("7", "7", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("11", "11", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1")

    ).toDF(RcuGasConnessioniDistr2PSchema.getValues: _*)
      .write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasConnessioniDistr2Path)
  }

  def createRcuGasBilanciamentoP(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    List(
      ("aaa", "", "150604000000000169", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "")
    ).toDF(RcuGasBilanciamentoPSchema.getValues: _*)
      .write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasBilanciamentoPath)
  }

  def createPrtIstatRegioneClimaticaP(): Unit = {
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

  override def createVRcuGasDistributoreP(): Unit = None

  override def createRcuAziendaP(): Unit = None

  override def createRcuGasUdbP(): Unit = None

  override def createSettleGasGasTds(): Unit = None

  override def createCaPreFinal(): Unit = None

  override def createCaFinal(): Unit = None
}
