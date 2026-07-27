package it.eng.au.sgsFlussoStoricoGas.component.controller

import it.eng.au.sgsFlussoStoricoGas.controller.PrevalidationController
import it.eng.au.sgsFlussoStoricoGas.controller.aggregazioni.udb.{UdbSwgAAggregationController, UdbVtgSAggregationController}
import it.eng.au.sgsFlussoStoricoGas.controller.aggregazioni.udd.UddSwgAAggregationController
import it.eng.au.sgsFlussoStoricoGas.schema.aggregazione.AggregatoreInfoDettSchema
import it.eng.au.sgsFlussoStoricoGas.schema.dailyConsumption.DailyConsumptionSchema
import it.eng.au.sgsFlussoStoricoGas.schema.perimetro.SgsPerimetroSchema
import it.eng.au.sgsFlussoStoricoGas.schema.prt.PrtIstatRegioneClimaticaPSchema
import it.eng.au.sgsFlussoStoricoGas.schema.rcugas.{RcuGasConnessioniDistr2PSchema, RcuGasPdrPSchema, RcuGasVarConvertitorePSchema, RcuGasVarMisuratorePSchema}
import it.eng.au.sgsFlussoStoricoGas.utility.EnvironmentSparkTest
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
import org.junit.Assert

class AggregationControllerTest extends EnvironmentSparkTest {

  //Controllers
  val udbVtgSAggregationController = new UdbVtgSAggregationController
  val uddSwgAAggregationController = new UddSwgAAggregationController
  val prevalController = new PrevalidationController

  //Schemas
  val perimetroSchema = new StructType()
    .add(SgsPerimetroSchema.n_id_pratica.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.t_tipo_pratica.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.d_data_decorrenza.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.t_codice_pdr.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.n_id_pdr.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.piva_udd_entrante.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.piva_udb_entrante.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.piva_udb_uscente.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.data_estrazione.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.t_trattamento.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.t_stato_perimetro.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.anno_mese_calcolo_perimetro.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.giorno_calcolo_perimetro.toString, StringType, nullable = true)
    .add(SgsPerimetroSchema.executionId.toString, LongType, nullable = true)

  val dailyConsumptionSchema = new StructType()
    .add(DailyConsumptionSchema.pdr.toString, StringType, nullable = true)
    .add(DailyConsumptionSchema.date.toString, StringType, nullable = true)
    .add(DailyConsumptionSchema.value.toString, DoubleType, nullable = true)
    .add(DailyConsumptionSchema.annoMese.toString, StringType, nullable = true)
    .add(DailyConsumptionSchema.treatment.toString, StringType, nullable = true)
    .add(DailyConsumptionSchema.pivaDistr.toString, StringType, nullable = true)
    .add(DailyConsumptionSchema.valueNotSterilized.toString, DoubleType, nullable = true)

  val rcuGasVarMisuratoreSchema = new StructType()
    .add(RcuGasVarMisuratorePSchema.n_id_pdr.toString, StringType, nullable = true)
    .add(RcuGasVarMisuratorePSchema.t_matricola_misuratore.toString, StringType, nullable = true)
    .add(RcuGasVarMisuratorePSchema.t_data_inst_misuratore.toString, StringType, nullable = true)
    .add(RcuGasVarMisuratorePSchema.t_misuratore_integrato.toString, StringType, nullable = true)
    .add(RcuGasVarMisuratorePSchema.t_classe_misuratore.toString, StringType, nullable = true)
    .add(RcuGasVarMisuratorePSchema.t_telegestito.toString, StringType, nullable = true)
    .add(RcuGasVarMisuratorePSchema.t_presenza_convertitore.toString, StringType, nullable = true)
    .add(RcuGasVarMisuratorePSchema.t_tipo_misuratore.toString, StringType, nullable = true)
    .add(RcuGasVarMisuratorePSchema.n_coeff_correzione.toString, StringType, nullable = true)

  val rcuGasVarConvertitoreSchema = new StructType()
    .add(RcuGasVarConvertitorePSchema.n_id_pdr.toString, StringType, nullable = true)
    .add(RcuGasVarConvertitorePSchema.t_matricola_convertitore.toString, StringType, nullable = true)
    .add(RcuGasVarConvertitorePSchema.t_data_inst_convertitore.toString, StringType, nullable = true)

  val rcuGasConnessioniDistr2Schema = new StructType()
    .add(RcuGasConnessioniDistr2PSchema.n_id_pdr.toString, StringType, nullable = true)
    .add(RcuGasConnessioniDistr2PSchema.t_remi.toString, StringType, nullable = true)
    .add(RcuGasConnessioniDistr2PSchema.id_regione_climatica.toString, StringType, nullable = true)

  val prtIstatRegioneClimaticaSchema = new StructType()
    .add(PrtIstatRegioneClimaticaPSchema.t_regione_climatica.toString, StringType, nullable = true)
    .add(PrtIstatRegioneClimaticaPSchema.t_codice_istat.toString, StringType, nullable = true)
    .add(RcuGasPdrPSchema.n_id_pdr.toString, StringType, nullable = true)


  def testUdbVtgSAggregationController(): Unit = {

    //n_id_pratica, t_tipo_pratica, d_data_decorrenza, t_codice_pdr, n_id_pdr, piva_udd_entrante, piva_udb_entrante, piva_udb_uscente, data_estrazione , t_trattamento, t_stato_perimetro, anno_mese_calcolo_perimetro, giorno_calcolo_perimetro executionId
    val perimetroDF = Environment.getSpark.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(Seq(
        Row("pratica1", "VTG", "2024-12-07 00:00:00.0", "11111111111111", "nIdPdr1", "piva_udd_entrante1", "piva_udb_entrante1", "piva_udb_uscente1", "2024-12-07 00:00:00.0", "G", "OK", "202412", "01", 11111111L),
        Row("pratica2", "VTG", "2024-12-07 00:00:00.0", "11111122222222", "nIdPdr2", "piva_udd_entrante2", "piva_udb_entrante2", "piva_udb_uscente2", "2024-12-07 00:00:00.0", "G", "OK", "202412", "01", 11111112L),
        Row("pratica3", "VTG", "2024-12-07 00:00:00.0", "11111133333333", "nIdPdr3", "piva_udd_entrante3", "piva_udb_entrante3", "piva_udb_uscente3", "2024-12-07 00:00:00.0", "G", "OK", "202412", "01", 11111113L),
        Row("pratica4", "VTG", "2024-12-07 00:00:00.0", "11111144444444", "nIdPdr4", "piva_udd_entrante4", "piva_udb_entrante4", "piva_udb_uscente4", "2024-12-07 00:00:00.0", "G", "OK", "202412", "01", 11111114L),
        Row("pratica5", "VTG", "2024-12-07 00:00:00.0", "11111155555555", "nIdPdr5", "piva_udd_entrante5", "piva_udb_uguale5", "piva_udb_uguale5", "2024-12-07 00:00:00.0", "G", "OK", "202412", "01", 11111115L),
        Row("pratica6", "VTG", "2024-12-01 00:00:00.0", "11111166666666", "nIdPdr6", "piva_udd_entrante6", "piva_udb_entrante6", "piva_udb_uscente6", "2024-12-07 00:00:00.0", "G", "OK", "202412", "01", 11111116L),
        Row("pratica7", "VTG", "2024-12-01 00:00:00.0", "11111177777777", "nIdPdr7", "piva_udd_entrante7", "piva_udb_entrante7", "piva_udb_uscente7", "2024-12-07 00:00:00.0", "G", "KO", "202412", "01", 11111117L)
        //filtro KO ultima riga
      )),
      perimetroSchema
    )

    //pdr    //date    //value    //annoMese    //treatment    //pivaDistr    //valueNotSterilized
    val dailyConsumptionDF = Environment.getSpark.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(Seq(
        Row("11111111111111", "2024-09-01 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-02 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-03 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-04 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-05 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-06 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-07 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-08 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-09 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-10 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-11 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-12 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-13 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-14 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-15 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-16 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-17 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-18 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-19 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-20 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-21 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-22 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-23 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-24 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-25 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-26 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-27 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-28 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-29 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-09-30 00:00:00.0", 1.0, "202409", null, "piva_distr1", 100.0),
        Row("11111122222222", "2024-09-01 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-02 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-03 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-04 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-05 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-06 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-07 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-08 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-09 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-10 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-11 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-12 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-13 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-14 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-15 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-16 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-17 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-18 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-19 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-20 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-21 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-22 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-23 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-24 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-25 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-26 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-27 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-28 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-29 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-09-30 00:00:00.0", 1.0, "202409", "G", "piva_distr2", 100.0),
        Row("11111133333333", "2024-09-07 00:00:00.0", 1.0, "202409", "G", "piva_distr3", null),
        Row("11111133333333", "2024-09-08 00:00:00.0", 1.0, "202409", "G", "piva_distr3", null),
        Row("11111133333333", "2024-09-09 00:00:00.0", 1.0, "202409", "G", "piva_distr3", null),
        Row("11111133333333", "2024-09-02 00:00:00.0", 1.0, "202409", "G", "piva_distr3", null),
        Row("11111144444444", "2024-09-07 00:00:00.0", null, "202409", "G", "piva_distr4", null),
        Row("11111144444444", "2024-09-02 00:00:00.0", null, "202409", "G", "piva_distr4", null),
        Row("11111155555555", "2024-09-07 00:00:00.0", 1.0, "202409", "G", "piva_distr5", null),
        Row("11111155555555", "2024-09-01 00:00:00.0", 1.0, "202409", "G", "piva_distr5", null),
        Row("11111166666666", "2023-10-01 00:00:00.0", 1.0, "202310", "G", "piva_distr6", null), //filtro data decorr
        Row("11111177777777", "2024-09-01 00:00:00.0", 1.0, "202409", "G", "piva_distr7", null)  //filtro KO perimetro
      )),
      dailyConsumptionSchema
    )

    //n_id_pdr    //t_matricola_misuratore    //t_data_inst_misuratore    //t_misuratore_integrato    //t_classe_misuratore    //t_telegestito
    //t_presenza_convertitore    //t_tipo_misuratore    //n_coeff_correzione
    val rcuGasVarMisuratoreDF = Environment.getSpark.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(Seq(
        Row("nIdPdr1", "matr_mis1", "07/12/2024", "SI", "G4", "SI", "SI", "01", "1.0"),
        Row("nIdPdr2", "matr_mis2", "07/12/2023", "SI", "G4", "SI", "SI", "01", "1.0"),
        Row("nIdPdr3", "matr_mis3", "07/11/2024", "SI", "G4", "SI", "SI", "05", "1.0"),
        Row("nIdPdr4", "matr_mis4", "10/12/2024", "SI", "G4", "SI", "SI", "01", "1.0"),
        Row("nIdPdr5", "matr_mis5", "07/08/2024", "SI", "G4", "SI", "SI", "01", "1.0")
      )),
      rcuGasVarMisuratoreSchema
    )

    //n_id_pdr    //t_matricola_convertitore    //t_data_inst_convertitore
    val rcuGasVarConvertitoreDF = Environment.getSpark.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(Seq(
        Row("nIdPdr1", "matr_conv1", "07/12/2024"),
        Row("nIdPdr2", "matr_conv2", "08/11/2024"),
        Row("nIdPdr3", "matr_conv3", "10/11/2024"),
        Row("nIdPdr4", "matr_conv4", "11/11/2024"),
        Row("nIdPdr5", "matr_conv5", "12/11/2024")
      )),
      rcuGasVarConvertitoreSchema
    )

    //n_id_pdr    //t_remi   //id reg clim
    val rcuGasConnessioniDistr2DF = Environment.getSpark.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(Seq(
        Row("nIdPdr1", "t_remi1", "50"),
        Row("nIdPdr2", "t_remi2", "50"),
        Row("nIdPdr3", "t_remi3", "50"),
        Row("nIdPdr4", "t_remi4", "50"),
        Row("nIdPdr5", "t_remi5", "50")
      )),
      rcuGasConnessioniDistr2Schema
    )

    val result = udbVtgSAggregationController.getAggregatoreInfoDett(perimetroDF, dailyConsumptionDF, rcuGasVarMisuratoreDF, rcuGasVarConvertitoreDF, rcuGasConnessioniDistr2DF)
    val resultPreval = prevalController.executePrevalidation(result)

    resultPreval.orderBy(col(AggregatoreInfoDettSchema.cod_pdr)).show

    Assert.assertEquals(5, resultPreval.count)

  }

  def testUddSwgAAggregationController(): Unit = {

    //n_id_pratica, t_tipo_pratica, d_data_decorrenza, t_codice_pdr, n_id_pdr, piva_udd_entrante, piva_udb_entrante, piva_udb_uscente, data_estrazione , t_trattamento, t_stato_perimetro, anno_mese_calcolo_perimetro, giorno_calcolo_perimetro executionId
    val perimetroDF = Environment.getSpark.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(Seq(
        Row("pratica1", "SWG", "2024-12-01 00:00:00.0", "11111111111111", "nIdPdr1", "piva_udd_entrante1", "piva_udb_entrante1", "piva_udb_uscente1", "2024-12-07 00:00:00.0", "G", "OK", "202412", "01", 11111111L),
        Row("pratica2", "SWG", "2024-12-01 00:00:00.0", "11111122222222", "nIdPdr2", "piva_udd_entrante2", "piva_udb_entrante2", "piva_udb_uscente2", "2024-12-07 00:00:00.0", "G", "OK", "202412", "01", 11111112L),
        Row("pratica3", "SWG", "2024-12-01 00:00:00.0", "11111133333333", "nIdPdr3", "piva_udd_entrante3", "piva_udb_entrante3", "piva_udb_uscente3", "2024-12-07 00:00:00.0", "G", "OK", "202412", "01", 11111113L),
        Row("pratica4", "SWG", "2024-12-01 00:00:00.0", "11111144444444", "nIdPdr4", "piva_udd_entrante4", "piva_udb_entrante4", "piva_udb_uscente4", "2024-12-07 00:00:00.0", "G", "OK", "202412", "01", 11111114L),
        Row("pratica5", "SWG", "2024-12-01 00:00:00.0", "11111155555555", "nIdPdr5", "piva_udd_entrante5", "piva_udb_uguale5", "piva_udb_uguale5", "2024-12-07 00:00:00.0", "G", "OK", "202412", "01", 11111115L),
        Row("pratica6", "SWG", "2024-12-01 00:00:00.0", "11111166666666", "nIdPdr6", "piva_udd_entrante6", "piva_udb_entrante6", "piva_udb_uscente6", "2024-12-07 00:00:00.0", "G", "OK", "202412", "01", 11111116L),
        Row("pratica7", "SWG", "2024-12-01 00:00:00.0", "11111177777777", "nIdPdr7", "piva_udd_entrante7", "piva_udb_entrante7", "piva_udb_uscente7", "2024-12-07 00:00:00.0", "G", "KO", "202412", "01", 11111117L)
        //filtro KO ultima riga
      )),
      perimetroSchema
    )

    //pdr    //date    //value    //annoMese    //treatment    //pivaDistr    //valueNotSterilized
    val dailyConsumptionDF = Environment.getSpark.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(Seq(
        Row("11111111111111", "2024-11-01 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-02 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-03 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-04 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-05 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-06 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-07 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-08 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-09 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-10 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-11 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-12 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-13 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-14 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-15 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-16 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-17 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-18 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-19 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-20 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-21 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-22 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-23 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-24 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-25 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-26 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-27 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-28 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-29 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111111111111", "2024-11-30 00:00:00.0", 1.0, "202411", null, "piva_distr1", 100.0),
        Row("11111122222222", "2024-11-01 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-02 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-03 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-04 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-05 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-06 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-07 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-08 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-09 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-10 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-11 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-12 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-13 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-14 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-15 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-16 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-17 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-18 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-19 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-20 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-21 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-22 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-23 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-24 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-25 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-26 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-27 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-28 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-29 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111122222222", "2024-11-30 00:00:00.0", 1.0, "202411", "G", "piva_distr2", 100.0),
        Row("11111133333333", "2024-11-07 00:00:00.0", 1.0, "202411", "G", "piva_distr3", null),
        Row("11111133333333", "2024-11-08 00:00:00.0", 1.0, "202411", "G", "piva_distr3", null),
        Row("11111133333333", "2024-11-09 00:00:00.0", 1.0, "202411", "G", "piva_distr3", null),
        Row("11111133333333", "2024-11-02 00:00:00.0", 1.0, "202411", "G", "piva_distr3", null),
        Row("11111144444444", "2024-11-07 00:00:00.0", null, "202411", "G", "piva_distr4", null),
        Row("11111144444444", "2024-11-02 00:00:00.0", null, "202411", "G", "piva_distr4", null),
        Row("11111155555555", "2024-11-07 00:00:00.0", 1.0, "202411", "G", "piva_distr5", null),
        Row("11111155555555", "2024-11-01 00:00:00.0", 1.0, "202411", "G", "piva_distr5", null),
        Row("11111166666666", "2024-10-01 00:00:00.0", 1.0, "202410", "G", "piva_distr6", null), //filtro data decorr
        Row("11111177777777", "2024-11-01 00:00:00.0", 1.0, "202411", "G", "piva_distr7", null) //filtro KO
      )),
      dailyConsumptionSchema
    )

    //n_id_pdr    //t_matricola_misuratore    //t_data_inst_misuratore    //t_misuratore_integrato    //t_classe_misuratore    //t_telegestito
    //t_presenza_convertitore    //t_tipo_misuratore    //n_coeff_correzione
    val rcuGasVarMisuratoreDF = Environment.getSpark.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(Seq(
        Row("nIdPdr1", "matr_mis1", "07/12/2024", "SI", "G10", "SI", "SI", "02", "1.0"),
        Row("nIdPdr2", "matr_mis2", "07/12/2024", "SI", "G10", "SI", "SI", "02", "1.0"),
        Row("nIdPdr3", "matr_mis3", "07/12/2024", "SI", "F50", "SI", "SI", "02", "1.0"),
        Row("nIdPdr4", "matr_mis4", "07/12/2024", "SI", "G10", "SI", "SI", "02", "1.0"),
        Row("nIdPdr5", "matr_mis5", "07/12/2024", "SI", "G10", "SI", "SI", "02", "1.0")
      )),
      rcuGasVarMisuratoreSchema
    )

    //n_id_pdr    //t_matricola_convertitore    //t_data_inst_convertitore
    val rcuGasVarConvertitoreDF = Environment.getSpark.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(Seq(
        Row("nIdPdr1", "matr_conv1", "07/11/2024"),
        Row("nIdPdr2", "matr_conv2", "07/11/2024"),
        Row("nIdPdr3", "matr_conv3", "07/11/2024"),
        Row("nIdPdr4", "matr_conv4", "07/11/2024"),
        Row("nIdPdr5", "matr_conv5", "07/11/2024")
      )),
      rcuGasVarConvertitoreSchema
    )

    //n_id_pdr    //t_remi  //id_regione_climatica
    val rcuGasConnessioniDistr2DF = Environment.getSpark.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(Seq(
        Row("nIdPdr1", "t_remi1", "30"),
        Row("nIdPdr2", "t_remi2", "30"),
        Row("nIdPdr3", "t_remi3", "30"),
        Row("nIdPdr4", "t_remi4", "30"),
        Row("nIdPdr5", "t_remi5", "30")
      )),
      rcuGasConnessioniDistr2Schema
    )

    val result = uddSwgAAggregationController.getAggregatoreInfoDett(perimetroDF, dailyConsumptionDF, rcuGasVarMisuratoreDF, rcuGasVarConvertitoreDF, rcuGasConnessioniDistr2DF)
    val resultPreval = prevalController.executePrevalidation(result)

    resultPreval.orderBy(col(AggregatoreInfoDettSchema.cod_pdr)).show

    Assert.assertEquals(5, resultPreval.count)
  }

}
