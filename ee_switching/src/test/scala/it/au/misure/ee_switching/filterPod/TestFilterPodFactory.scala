package it.au.misure.ee_switching.filterPod

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import it.au.misure.ee_switching.args.FlowArgsConfig
import it.au.misure.ee_switching.model.schema.hive.{FunzionaliSchema, StoriciSchema}
import it.au.misure.ee_switching.utility.Constants.{FUNZIONALI, STORICI}
import it.au.misure.ee_switching.utility.environment.Environment
import it.au.misure.ee_switching.utility.{EnvironmentSparkTest}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.{Assert, Test}

class TestFilterPodFactory extends EnvironmentSparkTest {

  var dfFunzionali: DataFrame = null
  var dfStorici: DataFrame = null

  override def setUp(): Unit = {

    super.setUp()

    val rddFunzionali = Environment.getSpark.sparkContext.parallelize(List(
      Row("2020-12-01", false, "202012", "10000000000", "01000000001", "00000000000000", "F2G", "DP1234"),
      Row("2020-12-01", true, "202012", "10000000000", "01000000001", "00000000000001", "F2G", "DP1234"),
      Row("2020-12-15", true, "202012", "10000000001", "01000000002", "00000000000002", "F2G", "DP1234"),
      Row("2020-01-01", false, "202001", "10000000001", "01000000002", "00000000000003", "F2G", "DP1234"),
      Row("2020-01-01", true, "202001", "10000000001", "01000000003", "00000000000004", "F2G", "DP1234"),
      Row("2020-02-01", false, "202002", "10000000002", "01000000003", "00000000000005", "F2G", "DP1234"),
      Row("2020-02-01", true, "202002", "10000000002", "01000000004", "00000000000006", "F2G", "DP1234"),
      Row("2020-03-01", true, "202003", "10000000002", "01000000004", "00000000000007", "F2G", "DP1234"),
      Row("2020-04-01", true, "202004", "10000000002", "01000000005", "00000000000008", "F2G", "DP1234"),
      Row(LocalDate.now.toString, true, LocalDate.now.format(DateTimeFormatter.ofPattern("yyyyMM")), "10000000000", "01000000001", "00000000000009", "F2G", "DP1234"),
      Row(LocalDate.now.toString, false, LocalDate.now.format(DateTimeFormatter.ofPattern("yyyyMM")), "10000000001", "01000000002", "00000000000010", "F2G", "DP1234"),
      Row(LocalDate.now.plusMonths(1).toString, true, LocalDate.now.plusMonths(1).format(DateTimeFormatter.ofPattern("yyyyMM")), "10000000001", "01000000003", "00000000000011", "F2G", "DP1234"),
      Row(LocalDate.now.plusMonths(1).toString, false, LocalDate.now.plusMonths(1).format(DateTimeFormatter.ofPattern("yyyyMM")), "10000000002", "01000000003", "00000000000012", "F2G", "DP1234")))

    val schemaFunzionali = StructType(Array(
      StructField(FunzionaliSchema.d_data_decorrenza, StringType, nullable = true),
      StructField(FunzionaliSchema.is_nuova_attivazione, BooleanType, nullable = true),
      StructField(FunzionaliSchema.annomese_sw, StringType, nullable = true),
      StructField(FunzionaliSchema.piva_distr, StringType, nullable = true),
      StructField(FunzionaliSchema.piva_udd, StringType, nullable = true),
      StructField(FunzionaliSchema.pod14, StringType, nullable = true),
      StructField(FunzionaliSchema.nome_flusso, StringType, nullable = true),
      StructField(FunzionaliSchema.t_cod_contr_disp, StringType, nullable = true)
    ))

    dfFunzionali = Environment.getSpark.sqlContext.createDataFrame(rddFunzionali, schemaFunzionali).cache()


    val rddStorici = Environment.getSpark.sparkContext.parallelize(List(
      Row("2020-12-01", 2020, 10, "O", "202012", "10000000000", "01000000001", "00000000000000", "S2G", "DP1234"),
      Row("2020-12-01", 2020, 10, "O", "202012", "10000000000", "01000000001", "00000000000001", "S2G", "DP1234"),
      Row("2020-12-15", 2020, 10, "O", "202012", "10000000000", "01000000001", "00000000000002", "S2G", "DP1234"),
      Row("2020-01-01", 2019, 10, "O", "202001", "10000000000", "01000000001", "00000000000003", "S2G", "DP1234"),
      Row("2020-01-01", 2019, 10, "O", "202001", "10000000000", "01000000001", "00000000000004", "S2G", "DP1234"),
      Row("2020-02-01", 2019, 10, "O", "202002", "10000000000", "01000000002", "00000000000005", "S2G", "DP1234"),
      Row("2020-02-01", 2019, 10, "O", "202002", "10000000000", "01000000002", "00000000000006", "S2G", "DP1234"),
      Row("2020-03-01", 2019, 10, "O", "202003", "10000000000", "01000000002", "00000000000007", "S2G", "DP1234"),
      Row("2020-04-01", 2019, 10, "O", "202004", "10000000000", "01000000002", "00000000000008", "S2G", "DP1234"),
      Row("2020-12-01", 2020, 10, "F", "202012", "10000000001", "01000000002", "00000000000009", "S2G", "DP1234"),
      Row("2020-12-01", 2020, 10, "F", "202012", "10000000001", "01000000002", "00000000000010", "S2G", "DP1234"),
      Row("2020-12-15", 2020, 10, "F", "202012", "10000000001", "01000000003", "00000000000011", "S2G", "DP1234"),
      Row("2020-01-01", 2019, 10, "F", "202001", "10000000001", "01000000003", "00000000000012", "S2G", "DP1234"),
      Row("2020-01-01", 2019, 10, "F", "202001", "10000000001", "01000000003", "00000000000013", "S2G", "DP1234"),
      Row("2020-02-01", 2019, 10, "F", "202002", "10000000001", "01000000003", "00000000000014", "S2G", "DP1234"),
      Row("2020-02-01", 2019, 10, "F", "202002", "10000000001", "01000000003", "00000000000015", "S2G", "DP1234"),
      Row("2020-03-01", 2019, 10, "F", "202003", "10000000001", "01000000003", "00000000000016", "S2G", "DP1234"),
      Row("2020-04-01", 2019, 10, "F", "202004", "10000000001", "01000000003", "00000000000017", "S2G", "DP1234"),
      Row(LocalDate.now.toString, LocalDate.now.minusMonths(2).format(DateTimeFormatter.ofPattern("yyyy")).toInt, LocalDate.now.minusMonths(2).format(DateTimeFormatter.ofPattern("MM")).toInt, "O", LocalDate.now.format(DateTimeFormatter.ofPattern("yyyyMM")), "10000000000", "01000000001", "00000000000018", "S2G", "DP1234"),
      Row(LocalDate.now.toString, LocalDate.now.minusMonths(1).format(DateTimeFormatter.ofPattern("yyyy")).toInt, LocalDate.now.minusMonths(1).format(DateTimeFormatter.ofPattern("MM")).toInt, "O", LocalDate.now.format(DateTimeFormatter.ofPattern("yyyyMM")), "10000000000", "01000000001", "00000000000018", "S2G", "DP1234"),
      Row(LocalDate.now.toString, LocalDate.now.minusMonths(3).format(DateTimeFormatter.ofPattern("yyyy")).toInt, LocalDate.now.minusMonths(3).format(DateTimeFormatter.ofPattern("MM")).toInt, "F", LocalDate.now.format(DateTimeFormatter.ofPattern("yyyyMM")), "10000000001", "01000000002", "00000000000019", "S2G", "DP1234"),
      Row(LocalDate.now.plusMonths(1).toString, LocalDate.now.minusMonths(2).format(DateTimeFormatter.ofPattern("yyyy")).toInt, LocalDate.now.minusMonths(2).format(DateTimeFormatter.ofPattern("MM")).toInt, "O", LocalDate.now.plusMonths(1).format(DateTimeFormatter.ofPattern("yyyyMM")), "10000000003", "01000000002", "00000000000020", "S2G", "DP1234"),
      Row(LocalDate.now.plusMonths(1).toString, LocalDate.now.minusMonths(3).format(DateTimeFormatter.ofPattern("yyyy")).toInt, LocalDate.now.minusMonths(3).format(DateTimeFormatter.ofPattern("MM")).toInt, "F", LocalDate.now.plusMonths(1).format(DateTimeFormatter.ofPattern("yyyyMM")), "10000000004", "01000000002", "00000000000021", "S2G", "DP1234")))

    val schemaStorici = StructType(Array(
      StructField(StoriciSchema.d_data_decorrenza, StringType, nullable = true),
      StructField(StoriciSchema.anno_misura, IntegerType, nullable = true),
      StructField(StoriciSchema.mese_misura, IntegerType, nullable = true),
      StructField(StoriciSchema.trattamento_online, StringType, nullable = true),
      StructField(StoriciSchema.annomese_sw, StringType, nullable = true),
      StructField(StoriciSchema.piva_distr, StringType, nullable = true),
      StructField(StoriciSchema.piva_udd, StringType, nullable = true),
      StructField(StoriciSchema.pod14, StringType, nullable = true),
      StructField(StoriciSchema.nome_flusso, StringType, nullable = true),
      StructField(StoriciSchema.dp, StringType, nullable = true)
    ))

    dfStorici = Environment.getSpark.sqlContext.createDataFrame(rddStorici, schemaStorici).cache
  }

  def assertThrows[E](f: => Unit)(implicit eType: scala.reflect.ClassTag[E]): Unit = {
    try {
      f
    } catch {
      case e: Exception =>
        if (eType.runtimeClass.isAssignableFrom(e.getClass))
          return;
    }
    throw new AssertionError("Expected error of type " + eType.runtimeClass.getName)
  }

  @Test
  def testFilter(): Unit = {

    val params1 = FlowArgsConfig(flowName = FUNZIONALI)
    val dfResult1 = FilterPodFactory.filter(dfFunzionali, FUNZIONALI, params1)
    Assert.assertEquals(2, dfResult1.count)

    val params2 = FlowArgsConfig(flowName = STORICI)
    val dfResult2 = FilterPodFactory.filter(dfStorici, STORICI, params2)
    dfResult2.show()
    Assert.assertEquals(3, dfResult2.count)


    val params3 = FlowArgsConfig(flowName = FUNZIONALI, listaPod = Seq[String]("00000000000009", "00000000000012"))
    val dfResult3 = FilterPodFactory.filter(dfFunzionali, FUNZIONALI, params3)
    Assert.assertEquals(2, dfResult3.count)

    val params4 = FlowArgsConfig(flowName = FUNZIONALI, listaPod = Seq[String]("00000000000009"), listaUdd = Seq[String]("01000000001", "01000000002"))
    val dfResult4 = FilterPodFactory.filter(dfFunzionali, FUNZIONALI, params4)
    Assert.assertEquals(1, dfResult4.count)

    val params5 = FlowArgsConfig(flowName = FUNZIONALI, listaPod = Seq[String]("00000000000000"), listaUdd = Seq[String]("01000000001"))
    val dfResult5 = FilterPodFactory.filter(dfFunzionali, FUNZIONALI, params5)
    Assert.assertEquals(0, dfResult5.count)

    val params6 = FlowArgsConfig(flowName = FUNZIONALI, listaDistributori = Seq[String]("10000000000"), listaUdd = Seq[String]("01000000001"))
    val dfResult6 = FilterPodFactory.filter(dfFunzionali, FUNZIONALI, params6)
    Assert.assertEquals(1, dfResult6.count)

    val params7 = FlowArgsConfig(flowName = FUNZIONALI, listaDateSW = Seq[LocalDate](LocalDate.parse("2020-02-01")), listaDistributori = Seq[String]("10000000002"), listaUdd = Seq[String]("01000000003"))
    val dfResult7 = FilterPodFactory.filter(dfFunzionali, FUNZIONALI, params7)
    Assert.assertEquals(1, dfResult7.count)

    val params8 = FlowArgsConfig(flowName = FUNZIONALI, runOrdinaria = false, listaDateSW = Seq[LocalDate](LocalDate.parse("2020-02-01")), listaDistributori = Seq[String]("10000000002"), listaUdd = Seq[String]("01000000005"))
    val dfResult8 = FilterPodFactory.filter(dfFunzionali, FUNZIONALI, params8)
    Assert.assertEquals(0, dfResult8.count)

    val params9 = FlowArgsConfig(flowName = FUNZIONALI, runOrdinaria = false, listaDateNA = Seq[LocalDate](LocalDate.parse("2020-03-01")), listaUdd = Seq[String]("01000000004"))
    val dfResult9 = FilterPodFactory.filter(dfFunzionali, FUNZIONALI, params9)
    Assert.assertEquals(1, dfResult9.count)

    val params10 = FlowArgsConfig(flowName = FUNZIONALI, runOrdinaria = false, listaDateNA = Seq[LocalDate](LocalDate.parse("2020-01-01"), LocalDate.parse("2020-12-01")))
    val dfResult10 = FilterPodFactory.filter(dfFunzionali, FUNZIONALI, params10)
    Assert.assertEquals(2, dfResult10.count)


    val params11 = FlowArgsConfig(flowName = STORICI, listaPod = Seq[String]("00000000000018", "00000000000020"))
    val dfResult11 = FilterPodFactory.filter(dfStorici, STORICI, params11)
    Assert.assertEquals(2, dfResult11.count)

    val params12 = FlowArgsConfig(flowName = STORICI, listaDistributori = Seq[String]("10000000001"))
    val dfResult12 = FilterPodFactory.filter(dfStorici, STORICI, params12)
    Assert.assertEquals(1, dfResult12.count)

    val params13 = FlowArgsConfig(flowName = STORICI, listaUdd = Seq[String]("01000000002"))
    val dfResult13 = FilterPodFactory.filter(dfStorici, STORICI, params13)
    Assert.assertEquals(2, dfResult13.count)

    val params14 = FlowArgsConfig(flowName = STORICI, listaDistributori = Seq[String]("10000000001"), listaUdd = Seq[String]("01000000002"))
    val dfResult14 = FilterPodFactory.filter(dfStorici, STORICI, params14)
    Assert.assertEquals(1, dfResult14.count)

    val params15 = FlowArgsConfig(flowName = STORICI, runOrdinaria = false, listaDateSW = Seq[LocalDate](LocalDate.parse("2020-02-01")), listaDistributori = Seq[String]("10000000000"), listaUdd = Seq[String]("01000000002"))
    val dfResult15 = FilterPodFactory.filter(dfStorici, STORICI, params15)
    Assert.assertEquals(2, dfResult15.count)

    val params16 = FlowArgsConfig(flowName = STORICI, runOrdinaria = false, listaDateSW = Seq[LocalDate](LocalDate.parse("2020-01-01")))
    val dfResult16 = FilterPodFactory.filter(dfStorici, STORICI, params16)
    Assert.assertEquals(4, dfResult16.count)

    val params17 = FlowArgsConfig(flowName = STORICI, runOrdinaria = false, listaDateSW = Seq[LocalDate](LocalDate.parse("2020-01-01")), listaCoppieDistrUdd = Seq(("10000000000","01000000001")))
    val dfResult17 = FilterPodFactory.filter(dfStorici, STORICI, params17)
    Assert.assertEquals(2, dfResult17.count)

  }

}
