package it.eng.au.sgsFlussoStoricoGas.component.controller

import it.eng.au.sgsFlussoStoricoGas.controller.perimetro.{PerimetroSwgAController, PerimetroSwgSController}
import it.eng.au.sgsFlussoStoricoGas.schema.perimetro.SgsPerimetroSchema
import it.eng.au.sgsFlussoStoricoGas.schema.perimetro.raw.{SgsPerimetroARawSchema, SgsPerimetroSRawSchema}
import it.eng.au.sgsFlussoStoricoGas.schema.rcugas.RcuGasVarTrattamentoPSchema
import it.eng.au.sgsFlussoStoricoGas.utility.EnvironmentSparkTest
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}
import org.junit.Assert

class PerimetroControllerTest extends EnvironmentSparkTest {

  val perimetroSController = new PerimetroSwgSController
  val perimetroAController = new PerimetroSwgAController

  val perimetroSwgSSchema = new StructType()
    .add(SgsPerimetroSchema.n_id_pratica.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.n_id_pdr.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.d_data_decorrenza.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.t_codice_pdr.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.piva_udd_entrante.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.piva_udb_entrante.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.piva_udb_uscente.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.data_estrazione.toString, StringType, nullable = true)

  val perimetroSRawSchema = new StructType()
    .add(SgsPerimetroSRawSchema.n_id_pratica.toString, StringType, nullable = true)
    .add(SgsPerimetroSRawSchema.t_stato_pratica.toString, StringType, nullable = true)
    .add(SgsPerimetroSRawSchema.b_ammissibile.toString, StringType, nullable = true)
    .add(SgsPerimetroSRawSchema.t_stato.toString, StringType, nullable = true)
    .add(SgsPerimetroSRawSchema.d_data_decorrenza.toString, StringType, nullable = true)
    .add(SgsPerimetroSRawSchema.t_codice_pdr.toString, StringType, nullable = true)
    .add(SgsPerimetroSRawSchema.n_id_pdr.toString, StringType, nullable = true)
    .add(SgsPerimetroSRawSchema.piva_udd_entrante.toString, StringType, nullable = true)
    .add(SgsPerimetroSRawSchema.piva_udb_entrante.toString, StringType, nullable = true)
    .add(SgsPerimetroSRawSchema.piva_udb_uscente.toString, StringType, nullable = true)
    .add(SgsPerimetroSRawSchema.data_estrazione.toString, StringType, nullable = true)

  val perimetroARawSchema = new StructType()
    .add(SgsPerimetroARawSchema.n_id_pratica.toString, StringType, nullable = true)
    .add(SgsPerimetroARawSchema.d_data_decorrenza.toString, StringType, nullable = true)
    .add(SgsPerimetroARawSchema.t_codice_pdr.toString, StringType, nullable = true)
    .add(SgsPerimetroARawSchema.n_id_pdr.toString, StringType, nullable = true)
    .add(SgsPerimetroARawSchema.piva_udb_entrante.toString, StringType, nullable = true)
    .add(SgsPerimetroARawSchema.data_estrazione.toString, StringType, nullable = true)

  val rcugasVarTrattamentoSchema = new StructType()
    .add(RcuGasVarTrattamentoPSchema.n_id_pdr.toString, StringType, nullable = true)
    .add(RcuGasVarTrattamentoPSchema.t_trattamento_settlement.toString, StringType, nullable = true)

  def testGetPerimetroSwgS(): Unit = {
    val annoEx = "2024"
    val meseEx = "12"
    val giornoEx = "02"

    //n_id_pratica,t_stato_pratica,b_ammissibile,t_stato,d_data_decorrenza,t_codice_pdr,n_id_pdr,piva_udd_entrante,piva_udb_entrante,piva_udb_uscente,data_estrazione
    val perimetroSwgSRaw = Environment.getSpark.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(Seq(
        Row("n1", "IN CORSO", "Y", "D", "2024-12-01 00:00:00.0" ,"A1", "1", "AA1", "AAA2", "AAAA4", "2024-11-07 00:00:00.0"),
        Row("n2", "IN CORSO", "Y", "D", "2024-12-01 00:00:00.0" ,"A2", "2", "AA2", "AAA2", "AAAA2", "2024-11-07 00:00:00.0"),
        Row("n3", "IN CORSO", "Y", "D", "2024-12-01 00:00:00.0" ,"A3", "3", "AA3", "AAA3", "AAAA3", "2024-11-07 00:00:00.0"),
        Row("n4", "IN CORSO", "Y", "D", "2024-12-01 00:00:00.0" ,"A4", "4", "AA4", "AAA4", "AAAA4", "2024-11-07 00:00:00.0"),
        Row("n5", "IN CORSO", "Y", "D", "2024-12-01 00:00:00.0" ,"A5", "5", "AA5", "AAA5", "AAAA5", "2024-11-07 00:00:00.0")
      )),
      perimetroSRawSchema
    )

    //n_id_pdr,t_trattamento_settlement
    val rcuGasVarTrattamento = Environment.getSpark.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(Seq(
        Row("1", "Y"),
        Row("2", null),
        Row("3", "G"),
        Row("4", "M"),
        Row("5", "G")
      )),
      rcugasVarTrattamentoSchema
    )

    val result = perimetroSController.getPerimetroS(perimetroSwgSRaw, rcuGasVarTrattamento, annoEx, meseEx, giornoEx)

    result.show

    Assert.assertEquals(5, result.count)

  }

  def testGetPerimetroSwgA(): Unit = {

    val annoEx = "2024"
    val meseEx = "12"
    val giornoEx = "02"

    //n_id_pratica ,n_id_pdr, d_data_decorrenza, t_codice_pdr ,piva_udd_entrante ,piva_udb_entrante ,piva_udb_uscente, data_Estrazione
    val perimetroSwgSOld = Environment.getSpark.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(Seq(
        Row("n1", "1", "2024-12-01 00:00:00.0", "A1", "AA1", "AAA1", "AAAA1", "2024-12-01 00:00:00.0"),
        Row("n2", "2", "2024-12-01 00:00:00.0", "A2", "AA2", "AAA2", "AAAA2", "2024-12-01 00:00:00.0"),
        Row("n3", "3", "2024-12-01 00:00:00.0", "A3", "AA3", "AAA3", "AAAA3", "2024-12-01 00:00:00.0"),
        Row("n4", "4", "2024-12-01 00:00:00.0", "A4", "AA4", "AAA4", "AAAA4", "2024-12-01 00:00:00.0"),
        Row("n5", "5", "2024-12-01 00:00:00.0", "A5", "AA5", "AAA5", "AAAA5", "2024-12-01 00:00:00.0"),
        Row("n6", "6", "2024-12-01 00:00:00.0", "A6", "AA6", "AAA6", "AAAA6", "2024-12-01 00:00:00.0")
      )),
      perimetroSwgSSchema
    )

    //n_id_pratica ,d_data_decorrenza ,t_codice_pdr ,n_id_pdr ,piva_udb_entrante ,data_estrazione
    val perimetroSwgARaw = Environment.getSpark.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(Seq(
        Row("n1", "2024-12-01 00:00:00.0", "A1", "1", "AAA1", "2024-11-07 00:00:00.0"),
        Row("n2", "2024-12-01 00:00:00.0", "A2", "2", "AAA2", "2024-11-07 00:00:00.0"),
        Row("n3", "2024-12-01 00:00:00.0", "A3", "3", "AAA3", "2024-11-07 00:00:00.0"),
        Row("n4", "2024-12-01 00:00:00.0", "A4", "4", "AAA4", "2024-11-07 00:00:00.0"),
        Row("n5", "2024-12-01 00:00:00.0", "A5", "5", "AAA5", "2024-11-07 00:00:00.0"),
        Row("n7", "2024-12-01 00:00:00.0", "A7", "7", "AAA7", "2024-11-07 00:00:00.0")
      )),
      perimetroARawSchema
    )

    //n_id_pdr,t_trattamento_settlement
    val rcuGasVarTrattamento = Environment.getSpark.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(Seq(
        Row("1", "Y"),
        Row("2", null),
        Row("3", "G"),
        Row("4", "M"),
        Row("5", "G"),
        Row("6", "G"),
        Row("7", "M")
      )),
      rcugasVarTrattamentoSchema
    )

    val result = perimetroAController.getPerimetroA(perimetroSwgARaw, rcuGasVarTrattamento, perimetroSwgSOld, annoEx, meseEx, giornoEx)

    result.show

    Assert.assertEquals(7, result.count)
  }

}
