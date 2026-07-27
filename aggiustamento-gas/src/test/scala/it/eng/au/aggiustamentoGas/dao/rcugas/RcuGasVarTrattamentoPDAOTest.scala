package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasMassivoP
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasVarTrattamentoPSchema
import it.eng.au.aggiustamentoGas.utility.constants.Treatment
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import junit.framework.TestCase
import org.apache.spark.sql.functions.col
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class RcuGasVarTrattamentoPDAOTest extends TestCase with EnvironmentSparkTest {

  def testPrepare(): Unit = {
    val sqlContext = Environment.getSpark.sqlContext
    import sqlContext.implicits._

    val rcuGasVarTrattamentoPDAO = new RcuGasVarTrattamentoPDAO

    val startDate = "202202"
    val endDate = "202203"
    val dateParser = DateTimeFormat.forPattern("yyyyMM")

    val startDatetime = dateParser.parseDateTime(startDate)
    val endDatetime = dateParser.parseDateTime(endDate).dayOfMonth().withMaximumValue()

    val tratment = List(
      ("1", "2022-01-01 00:00:00.0", "2022-01-31 00:00:00.0", "Y")
      , ("2", "2022-02-01 00:00:00.0", "2022-03-01 00:00:00.0", "Y")
      , ("3", "2022-01-01 00:00:00.0", "2022-01-31 00:00:00.0", "Y")
      , ("4", "2022-04-01 00:00:00.0", "2022-05-01 00:00:00.0", "Y")
      , ("5", "2022-03-31 00:00:00.0", "2022-05-01 00:00:00.0", "Y")
      , ("6", "2022-01-01 00:00:00.0", "2022-01-31 00:00:00.0", "Y")
      , ("7", "2022-01-01 00:00:00.0", "2022-07-01 00:00:00.0", "Y")
      , ("8", "2022-01-01 00:00:00.0", "2022-07-01 00:00:00.0", "Y")
      , ("9", "2022-01-01 00:00:00.0", null, "Y")
      , ("10", null, "2022-07-01 00:00:00.0", "Y")
      , ("11", "2022-01-01 00:00:00.0", "NULL", "Y")
      , ("12", "NULL", "2022-07-01 00:00:00.0", "Y")
      , ("13", null, null, "Y")
      , ("14", "NULL", "NULL", "Y")
      , ("15", "2022-01-01 00:00:00.0", "2022-02-01 00:00:00.0", "Y")
    ).toDF(
      RcuGasVarTrattamentoPSchema.n_id_pdr,
      RcuGasVarTrattamentoPSchema.d_data_inizio,
      RcuGasVarTrattamentoPSchema.d_data_fine,
      RcuGasVarTrattamentoPSchema.t_trattamento_settlement
    )

    val rcuMassivoActiveFurniture = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasMassivoP(startDate = startDatetime, endDate = endDatetime, tCodicePdr = "11", nIdPdr = "1", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = startDatetime, endDate = endDatetime, tCodicePdr = "22", nIdPdr = "2", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = startDatetime, endDate = endDatetime, tCodicePdr = "33", nIdPdr = "3", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = startDatetime, endDate = endDatetime, tCodicePdr = "44", nIdPdr = "4", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = startDatetime, endDate = endDatetime, tCodicePdr = "55", nIdPdr = "5", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = startDatetime, endDate = endDatetime, tCodicePdr = "66", nIdPdr = "6", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = startDatetime, endDate = endDatetime, tCodicePdr = "77", nIdPdr = "7", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = startDatetime, endDate = endDatetime, tCodicePdr = "88", nIdPdr = "8", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = startDatetime, endDate = endDatetime, tCodicePdr = "99", nIdPdr = "9", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = startDatetime, endDate = endDatetime, tCodicePdr = "1010", nIdPdr = "10", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = startDatetime, endDate = endDatetime, tCodicePdr = "1111", nIdPdr = "11", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = startDatetime, endDate = endDatetime, tCodicePdr = "1212", nIdPdr = "12", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = startDatetime, endDate = endDatetime, tCodicePdr = "1313", nIdPdr = "13", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = startDatetime, endDate = endDatetime, tCodicePdr = "1414", nIdPdr = "14", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
      , RcuGasMassivoP(startDate = startDatetime, endDate = endDatetime, tCodicePdr = "1515", nIdPdr = "15", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
    )
    )

    val res = rcuGasVarTrattamentoPDAO.prepare(tratment, startDate, endDate, rcuMassivoActiveFurniture).cache()
    res.show()

    Assert.assertEquals(0, res.filter(col(RcuGasVarTrattamentoPSchema.n_id_pdr) === "11").count())
    Assert.assertEquals(1, res.filter(col(RcuGasVarTrattamentoPSchema.n_id_pdr) === "22").count())
    Assert.assertEquals(0, res.filter(col(RcuGasVarTrattamentoPSchema.n_id_pdr) === "33").count())
    Assert.assertEquals(0, res.filter(col(RcuGasVarTrattamentoPSchema.n_id_pdr) === "44").count())
    Assert.assertEquals(1, res.filter(col(RcuGasVarTrattamentoPSchema.n_id_pdr) === "55").count())
    Assert.assertEquals(0, res.filter(col(RcuGasVarTrattamentoPSchema.n_id_pdr) === "66").count())
    Assert.assertEquals(1, res.filter(col(RcuGasVarTrattamentoPSchema.n_id_pdr) === "77").count())
    Assert.assertEquals(1, res.filter(col(RcuGasVarTrattamentoPSchema.n_id_pdr) === "88").count())
    Assert.assertEquals(1, res.filter(col(RcuGasVarTrattamentoPSchema.n_id_pdr) === "99").count())
    Assert.assertEquals(1, res.filter(col(RcuGasVarTrattamentoPSchema.n_id_pdr) === "1010").count())
    Assert.assertEquals(1, res.filter(col(RcuGasVarTrattamentoPSchema.n_id_pdr) === "1111").count())
    Assert.assertEquals(1, res.filter(col(RcuGasVarTrattamentoPSchema.n_id_pdr) === "1212").count())
    Assert.assertEquals(1, res.filter(col(RcuGasVarTrattamentoPSchema.n_id_pdr) === "1313").count())
    Assert.assertEquals(1, res.filter(col(RcuGasVarTrattamentoPSchema.n_id_pdr) === "1414").count())
    Assert.assertEquals(1, res.filter(col(RcuGasVarTrattamentoPSchema.n_id_pdr) === "1515").count())
  }

}
