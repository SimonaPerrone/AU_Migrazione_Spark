package it.sferanet.au.utility.environment

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.schema.{CaPreFinalSchema, RcuGasMassivoCaPSchema, RcuGasMassivoPSchema}
import it.sferanet.au.utilities.Environment
import it.sferanet.au.utilities.environment.{CcgFinEnvironment, CcgRicEnvironment}
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.junit.Assert

import java.io.File

class CcgSetEnvironmentTest extends EnvironmentSparkTest {
  def testCcgEnvironmens(): Unit = {
    FileUtils.deleteDirectory(new File("src/test/resources/local_test.db/"))
    Environment.setProperty("rcugas.massivo.tableName", "local_test.rcugas_massivo_ca_p_freeze")
    Environment.setProperty("ca_pre_final.table_name", "local_test.ca_pre_final")
    val sqlContext = Environment.getSpark.sqlContext
    import sqlContext.implicits._

    Environment.getSpark.sqlContext.sql("DROP TABLE IF EXISTS local_test.rcugas_massivo_ca_p_freeze")
    Environment.getSpark.sqlContext.sql("DROP TABLE IF EXISTS local_test.ca_pre_final")
    Environment.getSpark.sqlContext.sql("DROP DATABASE IF EXISTS local_test")
    Environment.getSpark.sqlContext.sql("CREATE DATABASE IF NOT EXISTS local_test LOCATION 'src/test/resources/local_test.db'")
    Environment.getSpark.sqlContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS local_test.rcugas_massivo_ca_p_freeze( t_codice_pdr string ) PARTITIONED BY (session string, execution_id bigint) LOCATION 'src/test/resources/local_test.db/rcugas_massivo_ca_p_freeze'")
    Environment.getSpark.sqlContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS local_test.ca_pre_final( codice_pdr string ) PARTITIONED BY (session string, tipo_trasmissione string, anno_competenza string, executionid bigint) LOCATION 'src/test/resources/local_test.db/ca_pre_final'")

    val rcuGasMassivo = Seq(
      ("pdr1", "CCG", 1654865841067L),
      ("pdr2", "CCG", 2654865841067L),
      ("pdr3", "CCG", 3654865841067L),
      ("pdr4", "CCG", 4654865841067L))
      .toDF(RcuGasMassivoPSchema.t_codice_pdr, "session", "execution_id")

    val caPreFinal = Seq(
      ("pdr1", "CCG", "AGG_FIN", "2022", 1654865841067L),
      ("pdr2", "CCG", "RIC", "2023", 2654865841067L),
      ("pdr3", "CDP", "AGG_FIN", "2022", 3654865841067L),
      ("pdr4", "CDP", "AGG_FIN", "2022", 4654865841067L),
      ("pdr5", "CDP", "AGG_FIN", "2023", 5654865841067L),
      ("pdr6", "CDP", "AGG_FIN", "2023", 6654865841067L),
      ("pdr7", "CDP", "RIC", "2022", 7654865841067L),
      ("pdr8", "CDP", "RIC", "2023", 8654865841067L))
      .toDF(CaPreFinalSchema.codice_pdr, CaPreFinalSchema.session, CaPreFinalSchema.tipo_trasmissione, CaPreFinalSchema.anno_competenza, CaPreFinalSchema.executionid)

    rcuGasMassivo.write.insertInto("local_test.rcugas_massivo_ca_p_freeze")
    caPreFinal.write.insertInto("local_test.ca_pre_final")

    finEnvironmentSetTest()
    finEnvironmentSetTest2()
    ricEnvironmentSetTest()
    ricEnvironmentSetTest2()
  }

  def finEnvironmentSetTest(): Unit = {
    val sysDate = DateTime.parse("2022-04-21")

    CcgFinEnvironment.set(sysDate)

    Assert.assertEquals(Environment.getFlowStartDate, "202010")
    Assert.assertEquals(Environment.getFlowEndDate, "202205")
    Assert.assertEquals(Environment.getFlowReceiveEndDate, "20220419")
    Assert.assertEquals(Environment.getTdsReceiveEndDate, "2022-04-19")
    Assert.assertEquals(Environment.getContractContuinityUpperBoundDate, "2022-06-01")
    Assert.assertEquals(Environment.getSession, "CCG")
    Assert.assertEquals(Environment.getTipoTrasmissione, "AGG_FIN")
    Assert.assertEquals(Environment.getMassivoAnnoCompetenza, "2023")
    Assert.assertEquals(Environment.getMassivoExecutionId, "4654865841067")
    Assert.assertEquals(Environment.getZInfDate, "2021-05-31")
    Assert.assertEquals(Environment.getZSupDate, "2022-05-31")
  }

  def finEnvironmentSetTest2(): Unit = {
    val sysDate = DateTime.parse("2022-11-21")

    CcgFinEnvironment.set(sysDate)

    Assert.assertEquals(Environment.getFlowStartDate, "202110")
    Assert.assertEquals(Environment.getFlowEndDate, "202305")
    Assert.assertEquals(Environment.getFlowReceiveEndDate, "20221119")
    Assert.assertEquals(Environment.getTdsReceiveEndDate, "2022-11-19")
    Assert.assertEquals(Environment.getContractContuinityUpperBoundDate, "2023-06-01")
    Assert.assertEquals(Environment.getSession, "CCG")
    Assert.assertEquals(Environment.getTipoTrasmissione, "AGG_FIN")
    Assert.assertEquals(Environment.getMassivoAnnoCompetenza, "2024")
    Assert.assertEquals(Environment.getMassivoExecutionId, "4654865841067")
    Assert.assertEquals(Environment.getZInfDate, "2022-05-31")
    Assert.assertEquals(Environment.getZSupDate, "2023-05-31")
  }

  def ricEnvironmentSetTest(): Unit = {
    val sysDate = DateTime.parse("2022-04-21")

    CcgRicEnvironment.set(sysDate)

    Assert.assertEquals(Environment.getFlowStartDate, "201910")
    Assert.assertEquals(Environment.getFlowEndDate, "202105")
    Assert.assertEquals(Environment.getFlowReceiveEndDate, "20220419")
    Assert.assertEquals(Environment.getTdsReceiveEndDate, "2022-04-19")
    Assert.assertEquals(Environment.getContractContuinityUpperBoundDate, "2021-06-01")
    Assert.assertEquals(Environment.getSession, "CCG")
    Assert.assertEquals(Environment.getTipoTrasmissione, "RIC")
    Assert.assertEquals(Environment.getMassivoAnnoCompetenza, "2022")
    Assert.assertEquals(Environment.getMassivoExecutionId, "4654865841067")
    Assert.assertEquals(Environment.getCaPreFinalExecutionId, "4654865841067")
    Assert.assertEquals(Environment.getZInfDate, "2020-05-31")
    Assert.assertEquals(Environment.getZSupDate, "2021-05-31")
  }

  def ricEnvironmentSetTest2(): Unit = {
    val sysDate = DateTime.parse("2022-11-21")

    CcgRicEnvironment.set(sysDate)

    Assert.assertEquals(Environment.getFlowStartDate, "202010")
    Assert.assertEquals(Environment.getFlowEndDate, "202205")
    Assert.assertEquals(Environment.getFlowReceiveEndDate, "20221119")
    Assert.assertEquals(Environment.getTdsReceiveEndDate, "2022-11-19")
    Assert.assertEquals(Environment.getContractContuinityUpperBoundDate, "2022-06-01")
    Assert.assertEquals(Environment.getSession, "CCG")
    Assert.assertEquals(Environment.getTipoTrasmissione, "RIC")
    Assert.assertEquals(Environment.getMassivoAnnoCompetenza, "2023")
    Assert.assertEquals(Environment.getMassivoExecutionId, "4654865841067")
    Assert.assertEquals(Environment.getCaPreFinalExecutionId, "6654865841067")
    Assert.assertEquals(Environment.getZInfDate, "2021-05-31")
    Assert.assertEquals(Environment.getZSupDate, "2022-05-31")
  }
}
