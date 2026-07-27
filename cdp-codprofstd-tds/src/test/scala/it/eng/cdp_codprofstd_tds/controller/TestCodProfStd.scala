package it.eng.cdp_codprofstd_tds.controller

import it.eng.cdp_codprofstd_tds.EnvironmentSparkTest
import it.eng.cdp_codprofstd_tds.controller.Prepare._
import it.eng.cdp_codprofstd_tds.dao.agg.CodProfStdDaTdsDao
import it.eng.cdp_codprofstd_tds.schema.CodProfStdDaTdsSchema
import it.eng.cdp_codprofstd_tds.utility.Constants.{DATA_RICEZIONE_FORMAT, DATE_FORMAT}
import it.eng.cdp_codprofstd_tds.utility.Environment
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.{from_unixtime, lit, unix_timestamp}

import java.io.File

class TestCodProfStd extends EnvironmentSparkTest {

  def testCodProfStd(): Unit = {
    FileUtils.deleteDirectory(new File("src/test/resources/output/parquet/"))

    class CodProfStdDaTdsDaoMock extends CodProfStdDaTdsDao {
      override val hdfsOutput: String = Environment.getCodProfStdDaTds
      override val tableName: String = Environment.getCodProfStdDaTds
    }

    val rcuGasMassivoDF = Environment.getSpark.sqlContext.read.format("csv").option("header", "true").option("nullValue", "NULL").load("src/test/resources/csv/rcugas_massivo_ca_test_freeze.csv")
    val gasTdsDF = Environment.getSpark.sqlContext.read.format("csv").option("header", "true").option("nullValue", "NULL").load("src/test/resources/csv/gas_tds_test.csv")
    val rcuGasBilanciamentoDF = Environment.getSpark.sqlContext.read.format("csv").option("header", "true").option("nullValue", "NULL").load("src/test/resources/csv/rcugas_bilanciamento_test.csv")
    val rcuGasConnessioniDistr2DF = Environment.getSpark.sqlContext.read.format("csv").option("header", "true").option("nullValue", "NULL").load("src/test/resources/csv/rcugas_connessioni_distr2_test.csv")
    val istatRegClima = Environment.getSpark.sqlContext.read.format("csv").option("header", "true").option("nullValue", "NULL").load("src/test/resources/csv/istat_regione_climatica_test.csv")
    val prtVsgDF = Environment.getSpark.sqlContext.read.format("csv").option("header", "true").option("nullValue", "NULL").load("src/test/resources/csv/prt_vsg_p.csv")
    val prtVtgDF = Environment.getSpark.sqlContext.read.format("csv").option("header", "true").option("nullValue", "NULL").load("src/test/resources/csv/prt_vtg_p.csv")
    val prtVsgAggRcuDF = Environment.getSpark.sqlContext.read.format("csv").option("header", "true").option("nullValue", "NULL").load("src/test/resources/csv/prt_vsg_agg_rcu_p.csv")
    val prtVtgAggRcuDF = Environment.getSpark.sqlContext.read.format("csv").option("header", "true").option("nullValue", "NULL").load("src/test/resources/csv/prt_vtg_agg_rcu_p.csv")
    val codProfStdDaTdsDao = new CodProfStdDaTdsDaoMock()

    val annoCompetenza = Environment.getProperty("anno.competenza")
    val executionId = Environment.getProperty("daterun").toLong
    val freezeDate = Environment.getProperty("freeze.date")
    val startDate = from_unixtime(unix_timestamp(lit(Environment.getProperty("start.data.ricezione")), DATA_RICEZIONE_FORMAT), DATE_FORMAT)
    val endDate = from_unixtime(unix_timestamp(lit(Environment.getProperty("end.data.ricezione")), DATA_RICEZIONE_FORMAT), DATE_FORMAT)
    val exclusionPdrPath = Environment.getProperty("exclusion.pdr.filter.csv.path")
    val exclusionIsActive = Environment.getProperty("exclusion.pdr.filter.isActive").toBoolean
    val forzaturaIsActive = Environment.getProperty("filterPdr.forzatura.isActive").toBoolean

    val rcuGasMassivo = prepareRcuGasMassivo(rcuGasMassivoDF, freezeDate)
    val rcuGasConnessioniDistr2 = prepareRcuGasConnessioniDistr2(rcuGasConnessioniDistr2DF, freezeDate)
    val rcuGasBilanciamento = prepareRcuGasBilanciamento(rcuGasBilanciamentoDF, freezeDate)
    val gasTds = prepareGasTds(gasTdsDF, startDate, endDate)
    val prtVsg = preparePrtVsg(prtVsgDF, startDate, endDate)
    val prtVtg = preparePrtVtg(prtVtgDF, startDate, endDate)
    val prtVsgAggRcu = preparePrtVsgAggRcu(prtVsgAggRcuDF)
    val prtVtgAggRcu = preparePrtVtgAggRcu(prtVtgAggRcuDF)
    val excludedPdrFromCsv = if (exclusionIsActive) prepareExclusionPdr(exclusionPdrPath) else Environment.getSpark.sqlContext.emptyDataFrame

    val codProfStdDaTds = Transform.transform(rcuGasMassivo, gasTds, istatRegClima, rcuGasConnessioniDistr2, rcuGasBilanciamento, prtVsg, prtVtg, prtVsgAggRcu, prtVtgAggRcu, excludedPdrFromCsv, exclusionIsActive, forzaturaIsActive, freezeDate, annoCompetenza, executionId)
    val outputDataFrame = codProfStdDaTds.selectExpr(CodProfStdDaTdsSchema.getValues: _*)

    codProfStdDaTdsDao.writeParquet(outputDataFrame)
  }

  def testRead(): Unit = {
    class CodProfStdDaTdsDaoMock extends CodProfStdDaTdsDao {
      override val hdfsOutput: String = Environment.getCodProfStdDaTds
      override val tableName: String = Environment.getCodProfStdDaTds
    }

    val outputCodProfStdDaTdsDao = new CodProfStdDaTdsDaoMock()

    Environment.getSpark.sqlContext.read.parquet(outputCodProfStdDaTdsDao.tableName).selectExpr(outputCodProfStdDaTdsDao.columns: _*).show()
  }
}