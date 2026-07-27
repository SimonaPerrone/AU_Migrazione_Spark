package it.eng.au.portaleConsumi.flow

import it.eng.au.portaleConsumi.EnvironmentSparkTest
import it.eng.au.portaleConsumi.common.{DatiComuni, DatiUtente1, DatiUtente2, DatiUtente3, DatiUtente4}
import it.eng.au.portaleConsumi.model.hive.rcugas.{RcugasClientefinalePModel, RcugasFornituraPModel}
import it.eng.au.portaleConsumi.model.mongodb.forniture.FornitureGasMongoDbModel
import it.eng.au.portaleConsumi.schema.misuregas.FornitureProcessiGasSchema
import it.eng.au.portaleConsumi.schema.mongodb.forniture.FornitureGasMongoDbSchema
import it.eng.au.portaleConsumi.schema.rcugas.{RcugasClientefinalePSchema, RcugasFornituraPSchema}
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, sum, when}
import org.junit.Assert

import java.sql.Timestamp

class FornitureProcessiGasFlowTest extends EnvironmentSparkTest {
  val spark = Environment.getSpark

  import spark.implicits._


  def testCalcolaFornitureGasUtente1(): Unit = {

    val expected = DatiUtente1.fornituraProcessiGas.head

    val result = new FornitureGasFlow().calcolaForniture(
      forniture = DatiUtente1.forniture.toDF(),
      cliente = DatiUtente1.clientefinale.toDF(),
      residenza = DatiUtente1.residenze.toDF(),
      pdr = DatiUtente1.pdr.toDF(),
      prelievi = DatiUtente1.datiprelievo.toDF(),
      misuratori = DatiUtente1.misuratori.toDF(),
      indirizzi = DatiUtente1.indirizzi.toDF(),
      venditori = DatiComuni.venditori.toDF(),
      aziende = DatiComuni.aziende.toDF(),
      connessioni = DatiUtente1.connessioni.toDF(),
      distributori = DatiComuni.distributori.toDF(),
      processi = DatiUtente1.processi.toDF(),
      offerte = DatiUtente1.offerte.toDF(),
      vulnerabilita = DatiUtente1.vulnerabilita.toDF(),
      dataCalcolo = "2022-12-31"
    ).collect()(0)

    val resultNoHash = result.copy(hashcode = null)
    Assert.assertEquals(expected, resultNoHash)
  }

  def testCalcolaFornitureGasUtente2(): Unit = {

    val expected_02800000011745 = DatiUtente2.fornituraProcessiGas.head
    val expected_02800000011746 = DatiUtente2.fornituraProcessiGas(1)

    val result = new FornitureGasFlow().calcolaForniture(
      forniture = DatiUtente2.forniture.toDF(),
      cliente = DatiUtente2.clientefinale.toDF(),
      residenza = DatiUtente2.residenze.toDF(),
      pdr = DatiUtente2.pdr.toDF(),
      prelievi = DatiUtente2.datiprelievo.toDF(),
      misuratori = DatiUtente2.misuratori.toDF(),
      indirizzi = DatiUtente2.indirizzi.toDF(),
      venditori = DatiComuni.venditori.toDF(),
      aziende = DatiComuni.aziende.toDF(),
      connessioni = DatiUtente2.connessioni.toDF(),
      distributori = DatiComuni.distributori.toDF(),
      processi = DatiUtente2.processi.toDF(),
      offerte = DatiUtente2.offerte.toDF(),
      vulnerabilita = DatiUtente2.vulnerabilita.toDF(),
      dataCalcolo = "2020-01-01"
    ).collect()

    Assert.assertEquals(2, result.length)
    Assert.assertEquals(expected_02800000011745, result(0).copy(hashcode = null))
    Assert.assertEquals(expected_02800000011746, result(1).copy(hashcode = null))
  }
  def testCalcolaFornitureGasUtente3(): Unit = {

    val expected_persona = DatiUtente3.fornituraProcessiGas.head
    val expected_piva_chiusa = DatiUtente3.fornituraProcessiGas(1)
    val expected_piva_aperta = DatiUtente3.fornituraProcessiGas(2)

    val result = new FornitureGasFlow().calcolaForniture(
      forniture = DatiUtente3.forniture.toDF(),
      cliente = DatiUtente3.clientefinale.toDF(),
      residenza = DatiUtente3.residenze.toDF(),
      pdr = DatiUtente3.pdr.toDF(),
      prelievi = DatiUtente3.datiprelievo.toDF(),
      misuratori = DatiUtente3.misuratori.toDF(),
      indirizzi = DatiUtente3.indirizzi.toDF(),
      venditori = DatiComuni.venditori.toDF(),
      aziende = DatiComuni.aziende.toDF(),
      connessioni = DatiUtente3.connessioni.toDF(),
      distributori = DatiComuni.distributori.toDF(),
      processi = DatiUtente3.processi.toDF(),
      offerte = DatiUtente3.offerte.toDF(),
      vulnerabilita = DatiUtente3.vulnerabilita.toDF(),
      dataCalcolo = "2020-01-01"
    ).cache

    Assert.assertEquals(3, result.count())
    Assert.assertEquals(expected_persona, result.where(col(FornitureProcessiGasSchema.codice_fornitura) === "n_id_fornitura3").head.copy(hashcode = null))
    Assert.assertEquals(expected_piva_chiusa, result.where(col(FornitureProcessiGasSchema.codice_fornitura) === "n_id_fornitura32").head.copy(hashcode = null))
    Assert.assertEquals(expected_piva_aperta, result.where(col(FornitureProcessiGasSchema.codice_fornitura) === "n_id_fornitura33").head.copy(hashcode = null))
  }


  def testConvertiInStrutturaMongoUtente1(): Unit = {
    val expected: FornitureGasMongoDbModel = DatiUtente1.fornituraGasMongodb
    val result: FornitureGasMongoDbModel = new FornitureGasFlow()
      .convertiInStrutturaMongo(DatiUtente1.fornituraProcessiGas.toDS())
      .collect()(0)

    Assert.assertEquals(expected.id, result.id)
    Assert.assertEquals(expected._id, result._id)
    Assert.assertEquals(expected.codice_fiscale, result.codice_fiscale)
    Assert.assertEquals(expected.anagrafica, result.anagrafica)
    Assert.assertEquals(expected.pdr.length, result.pdr.length)
    Assert.assertEquals(expected.pdr(0).forniture.length, result.pdr(0).forniture.length)
    Assert.assertEquals(expected.pdr(0).forniture.toList, result.pdr(0).forniture.toList)
    Assert.assertEquals(expected.pdr(0).processi.length, result.pdr(0).processi.length)
    Assert.assertEquals(expected.pdr(0).processi.toList, result.pdr(0).processi.toList)
  }

  def testRimuoviForniturePdrSenzaAttive(): Unit = {
    val ts = Timestamp.valueOf("2020-01-01 00:00:00")
    val forniture = Seq(
      RcugasFornituraPModel(n_id_fornitura = "x", n_id_pdr = "x", n_id_cliente = "2", d_data_fine = ts),
      RcugasFornituraPModel(n_id_fornitura = "1", n_id_pdr = "1", n_id_cliente = "1", d_data_fine = ts),
      RcugasFornituraPModel(n_id_fornitura = "2", n_id_pdr = "2", n_id_cliente = "1"),
      RcugasFornituraPModel(n_id_fornitura = "3", n_id_pdr = "2", n_id_cliente = "1", d_data_fine = ts)
    ).toDF()

    val clienti =  Seq(
      RcugasClientefinalePModel(n_id_cliente = "1", t_codice_fiscale = "cf1"),
      RcugasClientefinalePModel(n_id_cliente = "2", t_codice_fiscale = "cf2")
    ).toDF()

    val colFornituraAttiva = "_tmp_fornitura_attiva"
    val colPdrAttivo = "_tmp_pdr_attivo"
    val colIdCliente = "_tmp_cf_cliente"

    forniture
      .join(clienti, forniture(RcugasFornituraPSchema.n_id_cliente) === clienti(RcugasClientefinalePSchema.n_id_cliente), "INNER")
      .withColumn(colFornituraAttiva, when(forniture(RcugasFornituraPSchema.d_data_fine).isNull, 1).otherwise(0))
      // elimina forniture chiuse se pdr ha solo forniture chiuse per un cliente
      .withColumn(colIdCliente, coalesce(col(RcugasClientefinalePSchema.t_codice_fiscale), col(RcugasClientefinalePSchema.t_partita_iva)))
      .withColumn(colPdrAttivo, sum(col(colFornituraAttiva)).over(Window.partitionBy(col(colIdCliente), forniture(RcugasFornituraPSchema.n_id_pdr))))
      .where(col(colPdrAttivo) > 0)
      .show
  }

  def testConvertiInStrutturaMongoUtente1_new_feature(): Unit = {
    val DOT = "."
    val UNDERSCORE = "_"
    val expected: Seq[FornitureGasMongoDbModel] = DatiUtente4.fornituraGasMongodb
    val expectedDS: Dataset[FornitureGasMongoDbModel] = expected.toDS()
    val results: Dataset[FornitureGasMongoDbModel] = new FornitureGasFlow()
      .convertiInStrutturaMongo(DatiUtente4.fornituraProcessiGas.toDS())


    val result1Unionresult2 = results.filter(col(FornitureProcessiGasSchema.codice_fiscale ) ==="t_codice_fiscale_Utente1")
    val expectedSameCF = expectedDS.filter(col(FornitureGasMongoDbSchema.codice_fiscale)==="t_codice_fiscale_Utente1")

    Assert.assertEquals(expectedDS.count(), results.count())
    Assert.assertEquals(expectedSameCF.count(), result1Unionresult2.count())

    val result1 = results.filter(col(FornitureGasMongoDbSchema.codice_fiscale ) ==="t_codice_fiscale_Utente1"  and
      col(FornitureGasMongoDbSchema.codice_pdr) ==="t_codice_pdr1")


    val resilt1_ID = result1.select(col(FornitureGasMongoDbSchema._id)).collect()(0)(0)
    val resilt1ID = result1.select(col(FornitureGasMongoDbSchema.id)).collect()(0)(0)

    val resilt1CodiceFiscale = result1.select(col(FornitureGasMongoDbSchema.codice_fiscale)).collect()(0)(0)
    val resilt1CodicePDR = result1.select(col(FornitureGasMongoDbSchema.pdr + DOT + FornitureGasMongoDbSchema.codice_pdr).getItem(0))
      .collect()(0)(0)

    Assert.assertEquals(resilt1CodiceFiscale + UNDERSCORE + resilt1CodicePDR, resilt1_ID)
    Assert.assertEquals(resilt1CodiceFiscale + UNDERSCORE + resilt1CodicePDR, resilt1ID)

  }

}



