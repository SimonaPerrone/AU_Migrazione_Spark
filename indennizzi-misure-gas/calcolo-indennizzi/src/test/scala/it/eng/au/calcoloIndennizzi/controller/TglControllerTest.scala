package it.eng.au.calcoloIndennizzi.controller

import it.eng.au.calcoloIndennizzi.EnvironmentSparkTest
import it.eng.au.calcoloIndennizzi.dao.measure.TglDAO
import it.eng.au.calcoloIndennizzi.schema.cig.PdrGSettimoSchema
import it.eng.au.calcoloIndennizzi.schema.measure.TglSchema
import it.eng.au.calcoloIndennizzi.model.measure.Tgl
import it.eng.au.calcoloIndennizzi.utility.constants.Constants.{EFFETTIVA, TIMESTAMP_FORMAT, TIMESTAMP_MS_FORMAT}
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class TglControllerTest extends EnvironmentSparkTest {
  val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")

  def testGetInfo(): Unit = {
    Environment.setProperty("days.in.month", "4")

    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val df = List(
      ("pdr1", List("/mnt/localfile1"), 0, 4),
      ("pdr2", List("/mnt/localfile1"), 1, 3),
      ("pdr3", List("/mnt/localfile1"), 2, 2),
      ("pdr4", List("/mnt/localfile1"), 3, 1),
      ("pdr5", List("/mnt/localfile2"), 4, 0),
      ("pdr6", List("/mnt/localfile2"), 0, 4),
      ("pdr7", List("/mnt/localfile2"), 4, 3),
      ("pdr8", List("/mnt/localfile2"), 2, 2),
      ("pdr9", List("/mnt/localfile3"), 3, 1),
      ("pdr10", List("/mnt/localfile3"), 4, 0),
      ("pdr11", List("/mnt/localfile3"), 1, 3),
      ("pdr12", List("/mnt/localfile3"), 2, 2)
    ).toDF(
      TglSchema.cod_pdr,
      TglSchema.local_file,
      PdrGSettimoSchema.count_tgl_effettive,
      PdrGSettimoSchema.count_tgl_stimate)

    val result = TglController.getInfo(df)
    result.show

    Assert.assertEquals(12, result.count())
    Assert.assertEquals(7, result.columns.length)
    Assert.assertEquals(12, result.where(col(TglController.isTglOM1) === true).count())
    Assert.assertEquals(3, result.where(col(TglController.isTglOM2) === true).count())
    Assert.assertEquals(5, result.where(col(TglController.isTglOM3) === true).count())
    Assert.assertEquals(4, result.where(col(TglController.isTglOM2) === false && col(TglController.isTglOM3) === false).count())
  }

  def testGetTgl(): Unit = {
    val tglDAOMock = new TglDAOMock
    val tglDf = TglController.getTgl(tglDAOMock.readParquet)

    tglDf.show(false)

    Assert.assertEquals(8, tglDf.count())
    Assert.assertEquals(3, tglDf.columns.length)
    Assert.assertEquals(4, tglDf.where(col(TglSchema.tipo_lettura) === EFFETTIVA).count())
    Assert.assertEquals(1, tglDf.where(col(TglSchema.cod_pdr) === "pdr1").count())
    Assert.assertEquals(5, tglDf.where(col(TglSchema.cod_pdr) === "pdr9").count())
  }

  class TglDAOMock extends TglDAO {
    val sqlContext: SQLContext = Environment.sqlContext

    import sqlContext.implicits._

    override def readParquet: DataFrame = {
      Environment.sparkContext.parallelize(Seq(
        ("pdr1",  "01/10/2022", "P", "mis1",  "conv1",  "mis_giorn1",  "conv_giorn1",  Some(1.0),  Some(1.0),  "udd1",  "distr1",  "2022-11-07T23:59:59.999999", "E ",    "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "102022"),
        ("pdr1",  "01/10/2022", "P", "mis1",  "conv1",  "mis_giorn1",  "conv_giorn1",  Some(1.0),  Some(1.0),  "udd1",  "distr1",  "2022-11-02T08:00:00.000000", " E",    "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "102022"),
        ("pdr2",  "02/10/2022", "P", "mis2",  "conv2",  "mis_giorn2",  "conv_giorn2",  Some(2.0),  Some(2.0),  "udd2",  "distr2",  "2022-11-01T08:00:00.000000", "  S",   "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "102022"),
        ("pdr2",  "02/10/2022", "P", "mis2",  "conv2",  "mis_giorn2",  "conv_giorn2",  Some(2.0),  Some(2.0),  "udd2",  "distr2",  "2022-11-02T08:00:00.000000", "  S  ", "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "102022"),
        ("pdr3",  "03/10/2022", "P", "mis3",  "conv3",  "mis_giorn3",  "conv_giorn3",  Some(3.0),  Some(3.0),  "udd3",  "distr3",  "2022-11-08T00:00:00.000000", "E  ",   "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", null,        "102022"),

        ("pdr4",  "01/10/2022", "P", "mis5",  "conv5",  "mis_giorn5",  "conv_giorn5",  Some(5.0),  Some(5.0),  "udd5",  "distr5",  "2022-11-01T08:00:00.000000", " E ",   "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "BLOCCANTE", "102022"),
        ("pdr5",  "01/10/2022", "P", "mis7",  "conv7",  "mis_giorn7",  "conv_giorn7",  Some(7.0),  Some(7.0),  "udd7",  "distr7",  "2022-11-01T08:00:00.000000", "",      "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "102022"),
        ("pdr6",  "01/10/2022", "P", "mis8",  "conv8",  "mis_giorn8",  "conv_giorn8",  Some(8.0),  Some(8.0),  "udd8",  "distr8",  "2022-11-01T08:00:00.000000", "K",     "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "102022"),
        ("pdr7",  "01/10/2022", "P", "mis9",  "conv9",  "mis_giorn9",  "conv_giorn9",  Some(9.0),  Some(9.0),  "udd9",  "distr9",  "2022-11-01T08:00:00.000000", "S",     "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "092022"),
        ("pdr8",  "01/10/2022", "P", "mis10", "conv10", "mis_giorn10", "conv_giorn10", Some(10.0), Some(10.0), "udd10", "distr10", "2022-11-01T08:00:00.000000", "S",     "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "092022"),

        ("pdr9",  "01/10/2022", "P", "mis10", "conv10", "mis_giorn10", "conv_giorn10", Some(10.0), Some(10.0), "udd10", "distr10", "2022-11-01T08:00:00.000000", "S",     "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "102022"),
        ("pdr9",  "02/10/2022", "P", "mis10", "conv10", "mis_giorn10", "conv_giorn10", Some(10.0), Some(10.0), "udd10", "distr10", "2022-11-01T08:00:00.000000", "S",     "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "102022"),
        ("pdr9",  "03/10/2022", "P", "mis10", "conv10", "mis_giorn10", "conv_giorn10", Some(10.0), Some(10.0), "udd10", "distr10", "2022-11-01T08:00:00.000000", "S",     "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "102022"),
        ("pdr9",  "04/10/2022", "P", "mis10", "conv10", "mis_giorn10", "conv_giorn10", Some(10.0), Some(10.0), "udd10", "distr10", "2022-11-01T08:00:00.000000", "E",     "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml", "SI", "OK",        "102022"),
        ("pdr9",  "05/10/2022", "P", "mis10", "conv10", "mis_giorn10", "conv_giorn10", Some(10.0), Some(10.0), "udd10", "distr10", "2022-11-01T08:00:00.000000", "E",     "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml", "SI", "OK",        "102022"),

        ("pdr10", null,         "P", "mis10", "conv10", "mis_giorn10", "conv_giorn10", Some(10.0), Some(10.0), "udd10", "distr10", "2022-11-01T08:00:00.000000", null,    "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "092022"),
        ("pdr11", "",           "P", "mis10", "conv10", "mis_giorn10", "conv_giorn10", Some(10.0), Some(10.0), "udd10", "distr10", "2022-11-01T08:00:00.000000", "S",     "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "092022"),
        (null,    "01/10/2022", "P", "mis10", "conv10", "mis_giorn10", "conv_giorn10", Some(10.0), Some(10.0), "udd10", "distr10", "2022-11-01T08:00:00.000000", "S",     "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "092022"),
        ("   ",   "01/10/2022", "P", "mis6",  "conv6",  "mis_giorn6",  "conv_giorn6",  Some(6.0),  Some(6.0),  "udd6",  "distr6",  "2022-11-01T08:00:00.000000", "S",     "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "102022"),
        ("   ",   "01/10/2022", "P", "mis6",  "conv6",  "mis_giorn6",  "conv_giorn6",  None,       Some(6.0),  "udd6",  "distr6",  "2022-11-01T08:00:00.000000", "S",     "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "102022"),
        ("   ",   "2022-01-01", "P", null,    "conv6",  null,          "conv_giorn6",  Some(6.0),  Some(6.0),  "udd6",  "distr6",  "2022-11-01T08:00:00.000000", "S",     "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "102022"),
        ("   ",   "/10/2022",   "P", null,    null,     null,          null,           None,       None,       "udd6",  "distr6",  "2022-11-01T08:00:00.000000", "S",     "/mnt/isilonshare_gas/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml", "SI", "OK",        "102022")

      )).toDF(TglSchema.getValues: _*)
    }
  }

  def testModifyColumns(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val df = List(
      ("pdr1", "E", "01/10/2022", "1.0", "1.1", "mis1", null, "conv1", "conv1", "2022-10-01T08:00:00.000007", "102022"),
      ("pdr2", "E ", "01/10/2022", "2.0", "2.1", null, "mis2", null, "conv2", "2022-10-01T08:00:00.000700", "102022"),
      ("pdr3", "S", "02/10/2022", null, "3.1", "mis3", "mis3", "conv3", "conv3", "2022-10-01T08:00:00.00700", "102022"),
      ("pdr4", " E", "03/10/2022", "4.0", "4.1", null, null, "conv4g", "conv4", "2022-10-01T08:00:00.007000", "102022"),
      ("pdr5", "S ", "03/10/2022", "5.0", null, "mis5", "mis5", "conv5", null, "2022-10-01T08:00:00.070000", "092022"),
      ("pdr6", "  S  ", "03/10/2022", null, null, "mis6", "mis6", null, "conv6", "2022-10-01T08:00:00.700000", "082022")
    ).toDF(TglSchema.cod_pdr,
      TglSchema.tipo_lettura,
      TglSchema.data_comp,
      TglSchema.let_tot_prel,
      TglSchema.let_tot_conv,
      TglSchema.matr_mis_giornaliere,
      TglSchema.matr_mis,
      TglSchema.matr_conv_giornaliere,
      TglSchema.matr_conv,
      TglSchema.d_caricamento,
      TglSchema.mese_comp)

    val result = TglController.modifyColumns(df)
    result.show(false)

    Assert.assertEquals(6, result.count())
    Assert.assertEquals(11, result.columns.length)
    Assert.assertEquals(3, result.where(col(TglSchema.tipo_lettura) === EFFETTIVA).count())
    Assert.assertEquals(2, result.where(col(TglSchema.data_comp) === "2022-10-01").count())
    Assert.assertEquals(2, result.where(col(TglSchema.let_tot_prel).isNull).count())
    Assert.assertEquals(2, result.where(col(TglSchema.let_tot_conv).isNull).count())
    Assert.assertEquals(5, result.where(col(TglSchema.matr_mis).isNotNull).count())
    Assert.assertEquals(6, result.where(col(TglSchema.matr_conv).isNotNull).count())
    Assert.assertEquals(1, result.where(col(TglSchema.matr_conv) === "conv4g").count())
  }

  def testFilterAndSelect(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val df = List(
      ("pdr1", "S", "2022-11-07T08:00:00.000000", "/mnt/isilonshare/TMG_0/DISTRIBUTORE/TMG_0_0/2022/1031/localFile1", "pivaUtente", "pivaDistr", "E", "2022-10-01", Some(1.0), Some(1.1), "mis1", "mis1", "conv1", "conv1", "OK", "SI", "202210"),
      ("pdr1", "S", "2022-11-05T08:00:00.000000", "/mnt/isilonshare/TMG_0/DISTRIBUTORE/TMG_0_0/2022/1031/localFile1", "pivaUtente", "pivaDistr", "E", "2022-10-01", Some(2.0), Some(2.1), null, "mis2", null, "conv2", "OK", "SI", "202210"),
      ("pdr1", "P", "2022-11-02T08:00:00.000000", "/mnt/isilonshare/TMG_0/DISTRIBUTORE/TMG_0_0/2022/1031/localFile1", "pivaUtente", "pivaDistr", "S", "2022-10-02", None, Some(3.1), "mis3", "mis3", "conv3", "conv3", null, "SI", "202210"),
      ("pdr1", "S", "2022-11-08T00:00:00.000000", "/mnt/isilonshare/TMG_0/DISTRIBUTORE/TMG_0_0/2022/1031/localFile1", "pivaUtente", "pivaDistr", "S", "2022-10-02", None, Some(3.1), "mis3", "mis3", "conv3", "conv3", null, "SI", "202210"),
      ("pdr1", "S", "2022-11-01T08:00:00.000000", "/mnt/isilonshare/TMG_0/DISTRIBUTORE/TMG_0_0/2022/1031/localFile2", "pivaUtente", "pivaDistr", "E", "2022-10-02", Some(4.0), Some(4.1), null, null, "conv4", "conv4", "BLOCCANTE", "NO", "202210"),
      ("pdr1", "S", "2022-11-01T08:00:00.000000", "/mnt/isilonshare/TMG_0/DISTRIBUTORE/TMG_0_0/2022/1031/localFile2", "pivaUtente", "pivaDistr", "", "2022-10-03", Some(5.0), None, "mis5", "mis5", "conv5", "conv5", "OK", "NO", "202210"),
      ("pdr1", "S", "2022-11-01T08:00:00.000000", "/mnt/isilonshare/TMG_0/DISTRIBUTORE/TMG_0_0/2022/1031/localFile2", "pivaUtente", "pivaDistr", "S", "2022-10-03", None, None, "mis6", "mis6", null, "conv6", "OK", "NO", "202209"),
      ("pdr2", "S", "2022-11-07T08:00:00.000000", "/mnt/isilonshare/TMG_0/DISTRIBUTORE/TMG_0_0/2022/1031/localFile2", "pivaUtente", "pivaDistr", "E", "2022-10-01", Some(1.0), Some(1.1), "mis1", "mis1", "conv1", "conv1", "OK", "SI", "202210"),
      ("pdr2", "P", "2022-11-07T08:00:00.000000", "/mnt/isilonshare/TMG_0/DISTRIBUTORE/TMG_0_0/2022/1031/localFile2", "pivaUtente", "pivaDistr", "E", "2022-10-01", Some(1.0), Some(1.1), "mis1", "mis1", "conv1", "conv1", "OK", "SI", "202210")
    ).toDF(TglSchema.cod_pdr,
      TglSchema.raccolta,
      TglSchema.d_caricamento,
      TglSchema.local_file,
      TglSchema.piva_utente,
      TglSchema.piva_distr,
      TglSchema.tipo_lettura,
      TglSchema.data_comp,
      TglSchema.let_tot_prel,
      TglSchema.let_tot_conv,
      TglSchema.matr_mis_giornaliere,
      TglSchema.matr_mis,
      TglSchema.matr_conv_giornaliere,
      TglSchema.matr_conv,
      TglSchema.ammissibilita,
      TglSchema.val_dato_mens,
      TglSchema.mese_comp)

    val result = TglController.filterAndSelect(df)
    result.show

    Assert.assertEquals(6, result.count())
    Assert.assertEquals(13, result.columns.length)
    Assert.assertEquals(4, result.where(col(TglSchema.cod_pdr) === "pdr1").count())
    Assert.assertEquals(2, result.where(col(TglSchema.local_file) === "/mnt/isilonshare/TMG_0/DISTRIBUTORE/TMG_0_0/2022/1031/localFile2").count())
    Assert.assertEquals(4, result.where(col(TglSchema.tipo_lettura) === EFFETTIVA).count())
    Assert.assertEquals(4, result.where(col(TglSchema.data_comp) === "2022-10-01").count())
    Assert.assertEquals(2, result.where(col(TglSchema.ammissibilita).isNull).count())
    Assert.assertEquals(0, result.where(col(TglSchema.val_dato_mens) === "NO").count())
  }

  def testAggregateTgl(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val tgl = Environment.sparkContext.parallelize(Seq(
      ("pdr1", "/mnt/localfile1", "E"),
      ("pdr1", "/mnt/localfile1", "E"),
      ("pdr1", "/mnt/localfile2", "S"),
      ("pdr2", "/mnt/localfile3", "E"),
      ("pdr2", "/mnt/localfile4", "S"),
      ("pdr2", "/mnt/localfile4", "S"),
      ("pdr3", "/mnt/localfile5", "S"),
      ("pdr3", "/mnt/localfile5", "S"),
      ("pdr4", "/mnt/localfile6", "S")
    )).toDF(TglSchema.cod_pdr, TglSchema.local_file, TglSchema.tipo_lettura)

    val result = TglController.aggregateTgl(tgl)
    result.show(false)

    Assert.assertEquals(4, result.count())
    Assert.assertEquals(4, result.columns.length)
    Assert.assertEquals(1, result.where(col(TglSchema.cod_pdr) === "pdr1").count())
    Assert.assertEquals(2, result.where(col(TglSchema.cod_pdr) === lit("pdr2")).select(TglSchema.local_file).collect.apply(0).apply(0).toString.split(",").length)
    Assert.assertEquals(2, result.where(col(PdrGSettimoSchema.count_tgl_effettive) > 0).count())
    Assert.assertEquals(4, result.where(col(PdrGSettimoSchema.count_tgl_stimate) > 0).count())
  }

  def testGetPriorityMeasures(): Unit = {
    val measures = Environment.sparkContext.parallelize(Seq(
      Tgl("pdr1", Some(DateTime.parse("2022-10-27")), None, None, None, None, None, None, Some("/2020/0109/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), None, Some(DateTime.parse("2022-10-27T09:00:00.0")), None, None),
      Tgl("pdr1", Some(DateTime.parse("2022-10-27")), None, None, None, None, None, None, Some("/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), None, Some(DateTime.parse("2022-10-27T09:00:00.0")), None, None),
      Tgl("pdr1", Some(DateTime.parse("2022-10-27")), None, None, None, None, None, None, Some("/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151502_1.xml"), None, Some(DateTime.parse("2022-10-27T09:00:00.0")), None, None),

      Tgl("pdr2", Some(DateTime.parse("2022-10-28")), None, None, None, None, None, None, Some("/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151500_1.xml"), None, Some(DateTime.parse("2022-10-27T09:00:00.0")), None, None),
      Tgl("pdr2", Some(DateTime.parse("2022-10-28")), None, None, None, None, None, None, Some("/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), None, Some(DateTime.parse("2022-10-27T09:00:00.0")), None, None),
      Tgl("pdr2", Some(DateTime.parse("2022-10-28")), None, None, None, None, None, None, Some("/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"), None, Some(DateTime.parse("2022-10-27T09:00:00.0")), None, None),

      Tgl("pdr3", Some(DateTime.parse("2022-10-29")), None, None, None, None, None, None, Some("/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml"), None, Some(DateTime.parse("2022-10-27T09:00:00.0")), None, None),
      Tgl("pdr3", Some(DateTime.parse("2022-10-29")), None, None, None, None, None, None, Some("/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"), None, Some(DateTime.parse("2022-10-27T09:00:00.0")), None, None),
      Tgl("pdr3", Some(DateTime.parse("2022-10-29")), None, None, None, None, None, None, Some("/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"), None, Some(DateTime.parse("2022-10-27T10:00:00.0")), None, None)
    ))

    val result = TglController.getPriorityMeasures(measures)
    result.collect.foreach(println)

    Assert.assertEquals(3, result.count)
    Assert.assertEquals(1, result.filter(_.pdr == "pdr1").count)
    Assert.assertEquals(1, result.filter(_.pdr == "pdr2").count)
    Assert.assertEquals(1, result.filter(_.pdr == "pdr3").count)
  }

  def testRemoveDuplicateFlows(): Unit = {
    Environment.setProperty("filter.exclusion.enabled", "false")
    Environment.setProperty("filter.strongExclusion.enabled", "false")
    Environment.setProperty("filter.inclusion.pdr.enabled", "false")
    Environment.setProperty("filter.inclusion.id_distr.enabled", "false")
    Environment.setProperty("filter.inclusion.id_distr_piva_udd.enabled", "false")

    Environment.setProperty("filter.duplicateMeasures.enable", "true")

    val result = TglController.removeDuplicateFlows(tgl)
    val result1 = TglController.removeDuplicateFlows(tgl1)
    val result2 = TglController.removeDuplicateFlows(tgl2)
    val result3 = TglController.removeDuplicateFlows(tgl3)
    Environment.setProperty("filter.duplicateMeasures.groupByTimestamp.enable", "false")
    val result4 = TglController.removeDuplicateFlows(tgl4)

    Assert.assertEquals(0, result.filter(_.pdr.equals("pdr")).count())
    Assert.assertEquals(0, result1.filter(_.pdr.equals("pdr1")).count())
    Assert.assertEquals(1, result2.filter(_.pdr.equals("pdr2")).count())
    Assert.assertEquals(1, result3.filter(_.pdr.equals("pdr3")).count())
    Assert.assertEquals(2, result4.filter(_.pdr.equals("pdr4")).count())
  }

  // Case 1: flows are grouped by pdr, date and localFile, but they have different values in measure field. Result => 0 flows
  val tgl = Environment.sparkContext.parallelize(List(
    Tgl(pdr = "pdr", readType = None, date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None,
      serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), ammissibilita = None),
    Tgl(pdr = "pdr", readType = None, date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(1.0), converted = Some(2.0),
      serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva2"), serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), ammissibilita = None),
    Tgl(pdr = "pdr", readType = None, date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(3.0), converted = None,
      serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), ammissibilita = None)))

  // Case 1: they are grouped by pdr, date, localFile (there are two groups, 1-2 and 3-4): all the groups have the same fields. Result => 4 flows
  // Case 2: they are grouped by pdr, date, timestampLocalFile: they have different fileNames => 0 flows
  val tgl1 = Environment.sparkContext.parallelize(List(Tgl(pdr = "pdr1", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
    pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("01/01/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), readType = Some('S'), isValid = None, ammissibilita = None),
    Tgl(pdr = "pdr1", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
      pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("02/02/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), readType = Some('S'), isValid = None, ammissibilita = None),
    Tgl(pdr = "pdr1", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
      pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("03/03/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"), readType = Some('S'), isValid = None, ammissibilita = None),
    Tgl(pdr = "pdr1", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
      pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("04/04/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"), readType = Some('S'), isValid = None, ammissibilita = None)))

  // Case 1: they are grouped by pdr, date, localFile (there are two groups, 1-2 and 3-4): the first group is discarded since measure is different. Result => 2 flows
  // Case 2: the remaining flows are grouped by timestampLocalFile and fileName is checked. Result => 2 flows
  // Case 3: the remaining flows are grouped by fileName, and localFiles are checked. Result => 2 flows
  // Case 4: the remaining flows are grouped by localFile, and everything but dCaricamento is checked. Then the most recent flow is chosen. Result => 1 flow (the last one)
  val tgl2 = Environment.sparkContext.parallelize(List(
    Tgl(pdr = "pdr2", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(1.0), converted = None, serialNumberMis = Some("ABC"),
      pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("01/01/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), isValid = None, ammissibilita = None, readType = Some('S')),
    Tgl(pdr = "pdr2", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
      pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("02/02/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), isValid = None, ammissibilita = None, readType = Some('S')),
    Tgl(pdr = "pdr2", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
      pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("03/03/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"), isValid = None, ammissibilita = None, readType = Some('S')),
    Tgl(pdr = "pdr2", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
      pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("04/04/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"), isValid = None, ammissibilita = None, readType = Some('S'))))

  // Case 1: they are grouped by pdr, date, localFile (there are three groups, 1-2, 3-4, 5): the first group is discarded since converted is different. Result => 3 flows
  // Case 2: the remaining flows are grouped by timestampLocalFile and fileName is checked. Result => 3 flows
  // Case 3: the remaining flows are grouped by fileName, and localFiles are checked. Since they are different, each flow with inconsistent pivaUtente is discarded. Result => 2 flows
  // Case 4: the remaining flows are grouped by localFile, and everything but dCaricamento is checked. Then the most recent flow is chosen. Result => 1 flow (the chosen flow is random, since dCaricamento is None)
  val tgl3 = Environment.sparkContext.parallelize(List(
    Tgl(pdr = "pdr3", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = Some(1.0), serialNumberMis = Some("ABC"),
      pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"),
      readType = None, isValid = None, ammissibilita = None),
    Tgl(pdr = "pdr3", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = Some(2.0), serialNumberMis = Some("ABC"),
      pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"),
      readType = None, isValid = None, ammissibilita = None),
    Tgl(pdr = "pdr3", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
      pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_piva4/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"),
      readType = None, isValid = None, ammissibilita = None),
    Tgl(pdr = "pdr3", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
      pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_piva4/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"),
      readType = None, isValid = None, ammissibilita = None),
    Tgl(pdr = "pdr3", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
      pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_piva5/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"),
      readType = None, isValid = None, ammissibilita = None)))

  //When case 2 is deactivated,
  // Case 1: they are grouped by pdr, date, localFile (there are two groups, 1-2 and 3-4): all the groups have the same fields. Result => 4 flows
  // Case 2: deactivated. Result => 4 flows
  // Case 3: the remaining flows are grouped by fileName, and localFiles are checked. Result => 4 flows
  // Case 4: the remaining flows are grouped by localFile, and everything but dCaricamento is checked. Then the most recent flows is chosen. Result => 2 flows, since they have the same dCaricamento
  val tgl4 = Environment.sparkContext.parallelize(List(Tgl(pdr = "pdr1", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
    pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("01/01/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), readType = Some('S'), isValid = None, ammissibilita = None),
    Tgl(pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
      pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("02/02/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), readType = Some('S'), isValid = None, ammissibilita = None),
    Tgl(pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
      pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("03/03/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"), readType = Some('S'), isValid = None, ammissibilita = None),
    Tgl(pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
      pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("04/04/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"), readType = Some('S'), isValid = None, ammissibilita = None)))

  //for testing other cases
  val tgl5 = Environment.sparkContext.parallelize(List(
    Tgl(pdr = "pdr5", readType = Some('E'), date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(1.0), converted = None,
      serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), ammissibilita = None),
    Tgl(pdr = "pdr5", readType = Some('E'), date = Some(formatter.parseDateTime("02/01/2021")), measure = Some(1.0), converted = None,
      serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva2"), serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), ammissibilita = None),
    Tgl(pdr = "pdr5", readType = Some('E'), date = Some(formatter.parseDateTime("03/01/2021")), measure = Some(1.0), converted = None,
      serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), ammissibilita = None),
    Tgl(pdr = "pdr5", readType = Some('S'), date = Some(formatter.parseDateTime("04/01/2021")), measure = Some(1.0), converted = None,
      serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"), ammissibilita = None),
    Tgl(pdr = "pdr5", readType = Some('S'), date = Some(formatter.parseDateTime("05/01/2021")), measure = Some(1.0), converted = None,
      serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"), ammissibilita = None),
    Tgl(pdr = "pdr6", readType = Some('E'), date = Some(formatter.parseDateTime("06/01/2021")), measure = Some(1.0), converted = None,
      serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_3.xml"), ammissibilita = None)))

  def testFormatting(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val df = Environment.sparkContext.parallelize(Seq(
      ("pdr", "2022-10-01T08:00:00.700000"),
      ("pdr", "2022-10-01T08:00:00.070000"),
      ("pdr", "2022-10-01T08:00:00.007000"),
      ("pdr", "2022-10-01T08:00:00.000700"),
      ("pdr", "2022-10-01T08:00:00.000070"),
      ("pdr", "2022-10-01T08:00:00.000007"),

      ("pdr", "2022-10-01T08:00:00.70000"),
      ("pdr", "2022-10-01T08:00:00.07000"),
      ("pdr", "2022-10-01T08:00:00.00700"),
      ("pdr", "2022-10-01T08:00:00.00070"),
      ("pdr", "2022-10-01T08:00:00.00007"),

      ("pdr", "2022-10-01T08:00:00.7000"),
      ("pdr", "2022-10-01T08:00:00.0700"),
      ("pdr", "2022-10-01T08:00:00.0070"),
      ("pdr", "2022-10-01T08:00:00.0007"),

      ("pdr", "2022-10-01T08:00:00.700"),
      ("pdr", "2022-10-01T08:00:00.070"),
      ("pdr", "2022-10-01T08:00:00.007"),

      ("pdr", "2022-10-01T08:00:00.70"),
      ("pdr", "2022-10-01T08:00:00.07"),

      ("pdr", "2022-10-01T08:00:00.0"),
      ("pdr", "2022-10-01T08:00:00.7"),

      ("pdr", "2022-10-01"),
      ("pdr", "2022-10-01 08:00:00.001"),
      ("pdr", "01/10/2022 08:00:00.001"),
      ("pdr", ""),
      ("pdr", null)

    )).toDF("pdr", "data")

    df.withColumn("unix_timestamp", unix_timestamp(col("data"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
      .withColumn("to_timestamp", to_timestamp(col("data"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
      .withColumn("to_date", to_date(col("data")))
      .withColumn("date_format_ms", date_format(col("data"), TIMESTAMP_MS_FORMAT))
      .withColumn("date_format", date_format(col("data"), TIMESTAMP_FORMAT))
      .withColumn("cast_timestamp", col("data").cast(TimestampType))
      .show(30, truncate = false)
  }
}