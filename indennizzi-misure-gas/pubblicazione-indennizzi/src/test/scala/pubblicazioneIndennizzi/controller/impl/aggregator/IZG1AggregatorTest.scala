package pubblicazioneIndennizzi.controller.impl.aggregator

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import it.eng.au.pubblicazioneIndennizzi.controller.impl.aggregator.IZG1Aggregator
import it.eng.au.pubblicazioneIndennizzi.model.AggregatoTotale
import pubblicazioneIndennizzi.EnvironmentSparkTest

class IZG1AggregatorTest extends EnvironmentSparkTest {

  def testAggregatore() : Unit = {
    val spark = Environment.spark
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")


    val df = spark.createDataFrame(sc.parallelize(
      List (
        AggregatoTotale(677026152,	"00795040153",	"SERENISSIMA GAS SPA",	"03773040138",	"Acinque Energia S.r.l.",	19,	2,	5,	6,
          100,	94.73699951171875,	19.73699951171875,	18.6200008392334,	14.25,	2.8499999046325684,	0.3799999952316284,	3.75,	0.8999999761581421,
          10,	20,	30,	"202208", 1667398),
        /*AggregatoTotale(677026152,	"00795040153",	"SERENISSIMA GAS SPA",	"03773040138",	"Acinque Energia S.r.l.",	19,	null,	null,	null,
          100,	94.73699951171875,	19.73699951171875,	18.6200008392334,	14.25,	2.8499999046325684,	0.3799999952316284,	3.75,	0.8999999761581421,
          0,	0,	0,	"202208", 1667398591)*/
        AggregatoTotale(744505572,  "12060630964",  "ROMEO GAS S.P.A.",  "06655971007", "Enel Energia S.p.A.", 11, 11, 6, 0,
          100.0, 54.54499816894531, 0.0, 10.779999732971191, 8.25, 1.649999976158142, 0.2199999988079071, -2.25, -1.649999976158142,
          0.0, 81.0, 19.799999237060547, "202208", 1667820069884L)
      )))

    IZG1Aggregator.specificRun(df, "")
  }


  def test1() : Unit = {
    val spark = Environment.spark
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    val df = spark.createDataFrame(sc.parallelize(
      List (
        AggregatoTotale(677026152,	"00000000000",	"IMPR DISTR 0",	"00000000000",	"UDD 0",	19,	2,	5,	6,
          100,	94.73699951171875,	19.73699951171875,	18.6200008392334,	14.25,	2.8499999046325684,	0.3799999952316284,	3.75,	0.8999999761581421,
          10,	20,	30,	"202208", 1667398),


        AggregatoTotale(677026154,	"00000000000",	"IMPR DISTR 0",	 "11111111111",	"UDD 1",	19,	3,	4,	5,
          100,	94.73699951171875,	19.73699951171875,	18.6200008392334,	14.25,	2.8499999046325684,	0.3799999952316284,	3.75,	0.8999999761581421,
          30,	20,	10,	"202208", 1667398591),


        AggregatoTotale(677026156,	"11111111111",	"IMPR DISTR 1",	 "00000000000",	"UDD 0",	19,	6,	7,	6,
          100,	94.73699951171875,	19.73699951171875,	18.6200008392334,	14.25,	2.8499999046325684,	0.3799999952316284,	3.75,	0.8999999761581421,
          10,	20,	30,	"202208", 1667398591)

      )))

    IZG1Aggregator.specificRun(df, "")
  }


  def testNullValues() : Unit = {
    val spark = Environment.spark
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    val df = spark.createDataFrame(sc.parallelize(
      List(
        AggregatoTotale(piva_distr = "000", piva_udd = null),
        AggregatoTotale(piva_distr =  null, piva_udd = "000"),
        AggregatoTotale(piva_distr =  null, piva_udd = null))))

    IZG1Aggregator.specificRun(df, "")
  }
}
