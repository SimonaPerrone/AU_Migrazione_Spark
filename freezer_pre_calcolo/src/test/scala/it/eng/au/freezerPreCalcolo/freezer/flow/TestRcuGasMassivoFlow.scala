package it.eng.au.freezerPreCalcolo.freezer.flow

import org.apache.spark.sql.{DataFrame, SQLContext}
import it.eng.au.freezerPreCalcolo.EnvironmentSparkTest
import it.eng.au.freezerPreCalcolo.dao.rcugas.{RcuGasConnessioniDistr2DAO, RcuGasMassivoDAO, RcuGasMassivoFrozenDAO, RcuGasTechFrozenDAO, RcuGasVarConvertitoreDAO, RcuGasVarMisuratoreDAO}
import it.eng.au.freezerPreCalcolo.schema.RcuGasMassivoFrozenSchema
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel
import org.junit.Assert
import java.io.File

import it.eng.au.freezerPreCalcolo.utility.environment.Environment

class TestRcuGasMassivoFlow extends EnvironmentSparkTest {

  def testFlow(): Unit = {
    FileUtils.deleteDirectory(new File("src/test/resources/output/parquet/"))
    val testFlowMock = new RcuGasMassivoFlowMock
    testFlowMock.runFreezer()
    val (rcuGasMassivoFrozen, rcuGasTechFrozen) = testRead()

    rcuGasMassivoFrozen.persist(StorageLevel.MEMORY_AND_DISK)
    rcuGasTechFrozen.persist(StorageLevel.MEMORY_AND_DISK)


    Assert.assertEquals(29, rcuGasMassivoFrozen.count)
    Assert.assertEquals(15, rcuGasMassivoFrozen.where(col(RcuGasMassivoFrozenSchema.id_regione_climatica).isNotNull).count)
    Assert.assertEquals(3, rcuGasMassivoFrozen.where(col(RcuGasMassivoFrozenSchema.n_id_pdr) === "150606000008334693").count)
    Assert.assertEquals(0, rcuGasMassivoFrozen.where(col(RcuGasMassivoFrozenSchema.n_id_pdr) === "150605000007110630").count)
    Assert.assertEquals(1, rcuGasMassivoFrozen.where(col(RcuGasMassivoFrozenSchema.n_id_pdr) === "180906000025536518").count)
    Assert.assertEquals(3, rcuGasMassivoFrozen.where(col(RcuGasMassivoFrozenSchema.id_regione_climatica) === "13").count)

  }

  class RcuGasMassivoFlowMock extends RcuGasMassivoFlow {
    override val rcuGasMassivoDAO: RcuGasMassivoDAO = new RcuGasMassivoDAOMock
    override val rcuGasConnessioniDistr2DAO: RcuGasConnessioniDistr2DAO = new RcuGasConnessioniDistr2DAOMock
    override val rcuGasVarMisuratoreDAO: RcuGasVarMisuratoreDAO = new RcuGasVarMisuratoreDAOMock
    override val rcuGasVarConvertitoreDAO: RcuGasVarConvertitoreDAO = new RcuGasVarConvertitoreDAOMock
    override val rcuGasMassivoFrozenDao: RcuGasMassivoFrozenDAO = new RcuGasMassivoFrozenDAOMock
    override val rcuGasTechFrozenDao: RcuGasTechFrozenDAO = new RcuGasTechFrozenDAOMock
  }

  class RcuGasMassivoDAOMock extends RcuGasMassivoDAO {
    val rcuGasMassivoCsvPath: String = Environment.getProperty("agg.rcuGasMassivo.csvPath")

    override def readParquet: DataFrame = {
      Environment.getSpark.sqlContext.read.format("csv").option("header", "true").option("nullValue", "NULL").load(rcuGasMassivoCsvPath)
    }
  }

  class RcuGasConnessioniDistr2DAOMock extends RcuGasConnessioniDistr2DAO {
    val rcuGasConnessioniDistr2CsvPath: String = Environment.getProperty("agg.rcuGasConnessioniDistr2.csvPath")

    override def readParquet: DataFrame = {
      Environment.getSpark.sqlContext.read.format("csv").option("header", "true").option("nullValue", "NULL").load(rcuGasConnessioniDistr2CsvPath)
    }
  }

  class RcuGasVarMisuratoreDAOMock extends RcuGasVarMisuratoreDAO {
    val rcuGasVarMisuratoreCsvPath: String = Environment.getProperty("agg.rcuGasVarMisuratore.csvPath")

    override def readParquet: DataFrame = {
      Environment.getSpark.sqlContext.read.format("csv").option("header", "true").option("nullValue", "NULL").load(rcuGasVarMisuratoreCsvPath)
    }
  }

  class RcuGasVarConvertitoreDAOMock extends RcuGasVarConvertitoreDAO {
    val rcuGasVarConvertitoreCsvPath: String = Environment.getProperty("agg.rcuGasVarConvertitore.csvPath")

    override def readParquet: DataFrame = {
      Environment.getSpark.sqlContext.read.format("csv").option("header", "true").option("nullValue", "NULL").load(rcuGasVarConvertitoreCsvPath)
    }
  }

  class RcuGasMassivoFrozenDAOMock extends RcuGasMassivoFrozenDAO {
    override val hdfsOutput: String = Environment.getProperty("hdfs.output.rcugas_massivo_freeze")
    override val parquetPath: String = Environment.getProperty("hdfs.output.rcugas_massivo_freeze")
  }

  class RcuGasTechFrozenDAOMock extends RcuGasTechFrozenDAO {
    override val hdfsOutput: String = Environment.getProperty("hdfs.output.rcugas_tech_freeze")
    override val parquetPath: String = Environment.getProperty("hdfs.output.rcugas_tech_freeze")
  }

  def testRead(): (DataFrame, DataFrame) = {
    val rcuGasMassivoFrozenDao = new RcuGasMassivoFrozenDAOMock
    val df = rcuGasMassivoFrozenDao.readParquet

    val rcuGasTechFrozenDao = new RcuGasTechFrozenDAOMock
    val df2 = rcuGasTechFrozenDao.readParquet

    (df, df2)
  }

  def testReadUnit(): Unit = {
    val rcuGasMassivoFrozenDao = new RcuGasMassivoFrozenDAOMock
    val df = rcuGasMassivoFrozenDao.readParquet

    df.show(100, truncate = false)
  }
}
