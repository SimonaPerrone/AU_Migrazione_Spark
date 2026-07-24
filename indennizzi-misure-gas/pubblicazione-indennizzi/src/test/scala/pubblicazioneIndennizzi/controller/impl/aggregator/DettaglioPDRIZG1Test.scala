package pubblicazioneIndennizzi.controller.impl.aggregator

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import it.eng.au.pubblicazioneIndennizzi.controller.impl.aggregator.DettaglioPDRIZG1
import it.eng.au.pubblicazioneIndennizzi.model.DettaglioPDR
import pubblicazioneIndennizzi.EnvironmentSparkTest

class DettaglioPDRIZG1Test extends EnvironmentSparkTest {
  def testAggregatore() : Unit = {
    val spark = Environment.spark
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")


    val df = spark.createDataFrame(sc.parallelize(
      List(
        DettaglioPDR(1812459843L, "06724610966", "2I RETE GAS S.P.A.", "01368720080", "UNOENERGY SPA", "61493456002357",
          "/mnt/isilonshare_gas/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_01368720080/2022/0907/06724610966_01368720080_202208_TGL_20220907000200_02_M.zip", "202208",
          1667820069884L),
        DettaglioPDR(544961781L,"05608890488","TOSCANA ENERGIA S.P.A.","12300020158","Eni Plenitude S.p.A. Società Benefit","00594200994855",
          "/mnt/isilonshare_gas/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12300020158/2022/0907/05608890488_12300020158_202208_TGL_20220907110000_1_M.zip","202208",
          1667820069884L),
        DettaglioPDR(108489526L,"00930530324","AcegasApsAmga Spa","00997630322","Estenergy SpA","03620000027908",
          "/mnt/isilonshare_gas/TMG_00930530324/DISTRIBUTORE/TMG_00930530324_00997630322/2022/0905/00930530324_00997630322_202208_TGL_20220905160332_1_M.zip,/mnt/isilonshare_gas/TMG_00930530324/DISTRIBUTORE/TMG_00930530324_00997630322/2022/0907/00930530324_00997630322_202208_TGL_20220907112336_1_M.zip","202208",1667820069884L)))
    )
    DettaglioPDRIZG1.specificRun(df, "")
  }

  def testAggregatore2() : Unit = {
    val spark = Environment.spark
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")


    val df = spark.createDataFrame(sc.parallelize(
      List(
        DettaglioPDR(1812459843L, "06724610966", "2I RETE GAS S.P.A.", "01368720080", "UNOENERGY SPA", "61493456002357",
          "/mnt/isilonshare_gas/TMG_06724610966/DISTRIBUTORE/TMG_06724610966_01368720080/2022/0907/06724610966_01368720080_202208_TGL_20220907000200_02_M.zip", "202208",
          1667820069884L))))

    DettaglioPDRIZG1.specificRun(df, "")
  }

  def test1() : Unit = {
    val spark = Environment.spark
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")


    val df = spark.createDataFrame(sc.parallelize(
      List(
        DettaglioPDR(piva_id = "000", piva_udd = null),
        DettaglioPDR(piva_id = null, piva_udd = "000"),
        DettaglioPDR(piva_id = null, piva_udd = null))))

    DettaglioPDRIZG1.specificRun(df, "")
  }

  def testNullValues() : Unit = {
    val spark = Environment.spark
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")


    val df = spark.createDataFrame(sc.parallelize(
      List(
        DettaglioPDR(piva_id = "000", piva_udd = null),
        DettaglioPDR(piva_id =  null, piva_udd = "000"),
        DettaglioPDR(piva_id =  null, piva_udd = null))))

    DettaglioPDRIZG1.specificRun(df, "")
  }

  def testMultipleFileNames() : Unit = {
    val spark = Environment.spark
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")


    val df = spark.createDataFrame(sc.parallelize(
      List(
        DettaglioPDR(nome_file = "file1.zip,file2.zip,file3.zip,file4.zip,file5.zip", piva_id = "000", piva_udd = "111",
          annomese = "202211")))
    )

    DettaglioPDRIZG1.specificRun(df, "")
  }

}
