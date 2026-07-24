package it.au.misure.calcolo_capacita

import it.au.misure.calcolo_capacita.component.hdao.CalcoloCtcFlowInputHDao
import it.au.misure.calcolo_capacita.component.hdao.model.{AnagraficaHDao, CalcoloConsumiSbgHDao, PerimetroPdrHDao, RCUGasMassivoPHDao}
import it.au.misure.calcolo_capacita.component.schema.ClgPdrCapacitaSchema
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant._
import it.au.misure.calcolo_capacita.flow.CalcoloCtcFlow
import it.au.misure.calcolo_capacita.utility.test_case.CreatorFactory
import it.au.misure.calcolo_capacita.utility.{Checker, ForBusinessTest}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, lit}

class CalcoloCtcFlowTest
  extends ForBusinessTest
    with Checker {


  test("testCaseA11 x=4 y=100")
  {
    val in = CreatorFactory.getTestCreator(CreatorFactory.testCasea1)


    val _hdaoMock = stub[CalcoloCtcFlowInputHDao]
    val anagraficaMock = stub[AnagraficaHDao]
    val perimetroMock = stub[PerimetroPdrHDao]
    val misureMock = stub[CalcoloConsumiSbgHDao]
    val rcuGasMassivoMock = stub[RCUGasMassivoPHDao]

    (anagraficaMock.getDataframe _).when(sqlContext).returns(in.getAnagrafica)
    (perimetroMock.getDataframe _).when(sqlContext).returns(in.getMisureInPerimetro)
    (misureMock.getDataframe _).when(sqlContext).returns(in.getMeasures)
    (rcuGasMassivoMock.getDataframe _).when(sqlContext).returns(in.getRCUGasMassivo.get)

    (_hdaoMock.getAnagraficaHDao _).when().returns(anagraficaMock)
    (_hdaoMock.getPerimetroPdrHDao _).when().returns(perimetroMock)
    (_hdaoMock.getCalcoloConsumiSbgHDao _).when().returns(misureMock)
    (_hdaoMock.getRCUGasMassivoPHDao _).when().returns(rcuGasMassivoMock)

    var row: Array[Row] = Array()
    val args: Array[String] = Array("2021/03/08", "4", "100", "false", "true")

    val result = CalcoloCtcFlow(args, _hdaoMock, executionId).run()
      .cache()

    result.show(1000, false)

    checkIfExistsPdr(result, ClgPdrCapacitaSchema.t_codice_pdr, "PDR3", false)

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR1"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR1"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-25 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "308.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null")


    )

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR2"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR2"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-12 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-15 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "15.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "150.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR4"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR4"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "280.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-25 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR5"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR5"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc2Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRX"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRX"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "100.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "1000.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRM"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRM"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRF"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRF"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    result.unpersist()


  }

  test("testCaseA12 x=5 y=100")
  {
    val in = CreatorFactory.getTestCreator(CreatorFactory.testCasea1)

    val _hdaoMock = stub[CalcoloCtcFlowInputHDao]
    val anagraficaMock = stub[AnagraficaHDao]
    val perimetroMock = stub[PerimetroPdrHDao]
    val misureMock = stub[CalcoloConsumiSbgHDao]
    val rcuGasMassivoMock = stub[RCUGasMassivoPHDao]

    (anagraficaMock.getDataframe _).when(sqlContext).returns(in.getAnagrafica)
    (perimetroMock.getDataframe _).when(sqlContext).returns(in.getMisureInPerimetro)
    (misureMock.getDataframe _).when(sqlContext).returns(in.getMeasures)
    (rcuGasMassivoMock.getDataframe _).when(sqlContext).returns(in.getRCUGasMassivo.get)

    (_hdaoMock.getAnagraficaHDao _).when().returns(anagraficaMock)
    (_hdaoMock.getPerimetroPdrHDao _).when().returns(perimetroMock)
    (_hdaoMock.getCalcoloConsumiSbgHDao _).when().returns(misureMock)
    (_hdaoMock.getRCUGasMassivoPHDao _).when().returns(rcuGasMassivoMock)

    var row: Array[Row] = Array()
    val args: Array[String] = Array("2021/03/08", "5", "100", "false", "true")

    val result = CalcoloCtcFlow(args, _hdaoMock, executionId).run().cache()

    checkIfExistsPdr(result, ClgPdrCapacitaSchema.t_codice_pdr, "PDR3", false)

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR1"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR1"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "31.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "341.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-02 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-01-29 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR2"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR2"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "15.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "150.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-15 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-11 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR4"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR4"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "280.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-24 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR5"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR5"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc2Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRX"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRX"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "100.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "1000.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRM"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRM"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRF"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRF"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))
    result.unpersist()
  }

  test("testCaseA13 x=9 y=100")
  {
    val in = CreatorFactory.getTestCreator(CreatorFactory.testCasea1)

    val _hdaoMock = stub[CalcoloCtcFlowInputHDao]
    val anagraficaMock = stub[AnagraficaHDao]
    val perimetroMock = stub[PerimetroPdrHDao]
    val misureMock = stub[CalcoloConsumiSbgHDao]
    val rcuGasMassivoMock = stub[RCUGasMassivoPHDao]

    (anagraficaMock.getDataframe _).when(sqlContext).returns(in.getAnagrafica)
    (perimetroMock.getDataframe _).when(sqlContext).returns(in.getMisureInPerimetro)
    (misureMock.getDataframe _).when(sqlContext).returns(in.getMeasures)
    (rcuGasMassivoMock.getDataframe _).when(sqlContext).returns(in.getRCUGasMassivo.get)

    (_hdaoMock.getAnagraficaHDao _).when().returns(anagraficaMock)
    (_hdaoMock.getPerimetroPdrHDao _).when().returns(perimetroMock)
    (_hdaoMock.getCalcoloConsumiSbgHDao _).when().returns(misureMock)
    (_hdaoMock.getRCUGasMassivoPHDao _).when().returns(rcuGasMassivoMock)

    var row: Array[Row] = Array()
    val args: Array[String] = Array("2021/03/08", "9", "100", "false", "true")

    val result = CalcoloCtcFlow(args, _hdaoMock, executionId).run().cache()

    checkIfExistsPdr(result, ClgPdrCapacitaSchema.t_codice_pdr, "PDR3", false)

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR1"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR1"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "311.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "3421.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR2"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR2"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "10.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "100.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-01-10 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-01-02 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR4"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR4"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "280.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-20 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR5"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR5"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc2Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRX"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRX"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "100.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "1000.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRM"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRM"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRF"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRF"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    result.unpersist()
  }

  test("testCaseA14 x=10 y=100")
  {
    val in = CreatorFactory.getTestCreator(CreatorFactory.testCasea1)

    val _hdaoMock = stub[CalcoloCtcFlowInputHDao]
    val anagraficaMock = stub[AnagraficaHDao]
    val perimetroMock = stub[PerimetroPdrHDao]
    val misureMock = stub[CalcoloConsumiSbgHDao]
    val rcuGasMassivoMock = stub[RCUGasMassivoPHDao]

    (anagraficaMock.getDataframe _).when(sqlContext).returns(in.getAnagrafica)
    (perimetroMock.getDataframe _).when(sqlContext).returns(in.getMisureInPerimetro)
    (misureMock.getDataframe _).when(sqlContext).returns(in.getMeasures)
    (rcuGasMassivoMock.getDataframe _).when(sqlContext).returns(in.getRCUGasMassivo.get)

    (_hdaoMock.getAnagraficaHDao _).when().returns(anagraficaMock)
    (_hdaoMock.getPerimetroPdrHDao _).when().returns(perimetroMock)
    (_hdaoMock.getCalcoloConsumiSbgHDao _).when().returns(misureMock)
    (_hdaoMock.getRCUGasMassivoPHDao _).when().returns(rcuGasMassivoMock)

    var row: Array[Row] = Array()
    val args: Array[String] = Array("2021/03/08", "10", "100", "false", "true")

    val result = CalcoloCtcFlow(args, _hdaoMock, executionId).run().cache()

    checkIfExistsPdr(result, ClgPdrCapacitaSchema.t_codice_pdr, "PDR3", false)

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR1"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR1"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "311.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "3421.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR2"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR2"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "10.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "100.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-01-10 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-01-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR4"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR4"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "280.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-19 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR5"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR5"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc2Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRX"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRX"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "100.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "1000.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRM"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRM"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRF"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRF"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    result.unpersist()


  }

  test("testCaseA19 x=11 y=100")
  {
    val in = CreatorFactory.getTestCreator(CreatorFactory.testCasea1)

    val _hdaoMock = stub[CalcoloCtcFlowInputHDao]
    val anagraficaMock = stub[AnagraficaHDao]
    val perimetroMock = stub[PerimetroPdrHDao]
    val misureMock = stub[CalcoloConsumiSbgHDao]
    val rcuGasMassivoMock = stub[RCUGasMassivoPHDao]

    (anagraficaMock.getDataframe _).when(sqlContext).returns(in.getAnagrafica)
    (perimetroMock.getDataframe _).when(sqlContext).returns(in.getMisureInPerimetro)
    (misureMock.getDataframe _).when(sqlContext).returns(in.getMeasures)
    (rcuGasMassivoMock.getDataframe _).when(sqlContext).returns(in.getRCUGasMassivo.get)

    (_hdaoMock.getAnagraficaHDao _).when().returns(anagraficaMock)
    (_hdaoMock.getPerimetroPdrHDao _).when().returns(perimetroMock)
    (_hdaoMock.getCalcoloConsumiSbgHDao _).when().returns(misureMock)
    (_hdaoMock.getRCUGasMassivoPHDao _).when().returns(rcuGasMassivoMock)

    var row: Array[Row] = Array()
    val args: Array[String] = Array("2021/03/08", "11", "100", "false", "true")

    val result = CalcoloCtcFlow(args, _hdaoMock, executionId).run().cache()

    checkIfExistsPdr(result, ClgPdrCapacitaSchema.t_codice_pdr, "PDR3", false)

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR1"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR1"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "311.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "3421.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR2"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR2"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "31.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "310.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-01-10 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR4"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR4"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "280.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-18 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR5"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR5"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc2Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRX"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRX"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "100.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "1000.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRM"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRM"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRF"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRF"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-11-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))
    result.unpersist()
  }

  test("testCaseA15 x=4 y=4 [X=Y]")
  {
    val in = CreatorFactory.getTestCreator(CreatorFactory.testCasea1)

    val _hdaoMock = stub[CalcoloCtcFlowInputHDao]
    val anagraficaMock = stub[AnagraficaHDao]
    val perimetroMock = stub[PerimetroPdrHDao]
    val misureMock = stub[CalcoloConsumiSbgHDao]
    val rcuGasMassivoMock = stub[RCUGasMassivoPHDao]

    (anagraficaMock.getDataframe _).when(sqlContext).returns(in.getAnagrafica)
    (perimetroMock.getDataframe _).when(sqlContext).returns(in.getMisureInPerimetro)
    (misureMock.getDataframe _).when(sqlContext).returns(in.getMeasures)
    (rcuGasMassivoMock.getDataframe _).when(sqlContext).returns(in.getRCUGasMassivo.get)

    (_hdaoMock.getAnagraficaHDao _).when().returns(anagraficaMock)
    (_hdaoMock.getPerimetroPdrHDao _).when().returns(perimetroMock)
    (_hdaoMock.getCalcoloConsumiSbgHDao _).when().returns(misureMock)
    (_hdaoMock.getRCUGasMassivoPHDao _).when().returns(rcuGasMassivoMock)

    var row: Array[Row] = Array()

    val args: Array[String] = Array("2021/03/08", "4", "4", "false", "true")

    val result = CalcoloCtcFlow(args, _hdaoMock, executionId).run().cache()

    checkIfExistsPdr(result, ClgPdrCapacitaSchema.t_codice_pdr, "PDR3", false)

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR1"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR1"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "308.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-25 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR2"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR2"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-25 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR4"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR4"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "280.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-25 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR5"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR5"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc2Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRX"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRX"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "100.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "1000.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-25 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRM"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRM"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-25 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRF"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRF"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-25 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    result.unpersist()
  }

  test("testCaseA16 x=40 y=60")
  {
    val in = CreatorFactory.getTestCreator(CreatorFactory.testCasea1)

    val _hdaoMock = stub[CalcoloCtcFlowInputHDao]
    val anagraficaMock = stub[AnagraficaHDao]
    val perimetroMock = stub[PerimetroPdrHDao]
    val misureMock = stub[CalcoloConsumiSbgHDao]
    val rcuGasMassivoMock = stub[RCUGasMassivoPHDao]

    (anagraficaMock.getDataframe _).when(sqlContext).returns(in.getAnagrafica)
    (perimetroMock.getDataframe _).when(sqlContext).returns(in.getMisureInPerimetro)
    (misureMock.getDataframe _).when(sqlContext).returns(in.getMeasures)
    (rcuGasMassivoMock.getDataframe _).when(sqlContext).returns(in.getRCUGasMassivo.get)

    (_hdaoMock.getAnagraficaHDao _).when().returns(anagraficaMock)
    (_hdaoMock.getPerimetroPdrHDao _).when().returns(perimetroMock)
    (_hdaoMock.getCalcoloConsumiSbgHDao _).when().returns(misureMock)
    (_hdaoMock.getRCUGasMassivoPHDao _).when().returns(rcuGasMassivoMock)

    var row: Array[Row] = Array()

    val args: Array[String] = Array("2021/03/08", "40", "60", "false", "true")

    val result = CalcoloCtcFlow(args, _hdaoMock, executionId).run().cache()
    checkIfExistsPdr(result, ClgPdrCapacitaSchema.t_codice_pdr, "PDR3", false)

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR1"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR1"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "311.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "3421.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR2"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR2"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR4"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR4"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "31.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "310.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-01-20 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR5"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR5"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc2Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRX"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRX"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "100.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "1000.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRM"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRM"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRF"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRF"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))
    result.unpersist()
  }

  test("testCaseA17 x=29 y=60")
  {
    val in = CreatorFactory.getTestCreator(CreatorFactory.testCasea1)

    val _hdaoMock = stub[CalcoloCtcFlowInputHDao]
    val anagraficaMock = stub[AnagraficaHDao]
    val perimetroMock = stub[PerimetroPdrHDao]
    val misureMock = stub[CalcoloConsumiSbgHDao]
    val rcuGasMassivoMock = stub[RCUGasMassivoPHDao]

    (anagraficaMock.getDataframe _).when(sqlContext).returns(in.getAnagrafica)
    (perimetroMock.getDataframe _).when(sqlContext).returns(in.getMisureInPerimetro)
    (misureMock.getDataframe _).when(sqlContext).returns(in.getMeasures)
    (rcuGasMassivoMock.getDataframe _).when(sqlContext).returns(in.getRCUGasMassivo.get)

    (_hdaoMock.getAnagraficaHDao _).when().returns(anagraficaMock)
    (_hdaoMock.getPerimetroPdrHDao _).when().returns(perimetroMock)
    (_hdaoMock.getCalcoloConsumiSbgHDao _).when().returns(misureMock)
    (_hdaoMock.getRCUGasMassivoPHDao _).when().returns(rcuGasMassivoMock)

    var row: Array[Row] = Array()

    val args: Array[String] = Array("2021/03/08", "29", "60", "false", "true")

    val result = CalcoloCtcFlow(args, _hdaoMock, executionId).run().cache()

    checkIfExistsPdr(result, ClgPdrCapacitaSchema.t_codice_pdr, "PDR3", false)

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR1"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR1"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "311.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "3421.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR2"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR2"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR4"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR4"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "31.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "310.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-01-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR5"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR5"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc2Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRX"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRX"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "100.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "1000.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRM"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRM"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRF"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRF"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    result.unpersist()
  }

  test("testCaseA18 x=61 y=60 [TEST X > Y]")
  {
    val in = CreatorFactory.getTestCreator(CreatorFactory.testCasea1)

    val _hdaoMock = stub[CalcoloCtcFlowInputHDao]
    val anagraficaMock = stub[AnagraficaHDao]
    val perimetroMock = stub[PerimetroPdrHDao]
    val misureMock = stub[CalcoloConsumiSbgHDao]
    val rcuGasMassivoMock = stub[RCUGasMassivoPHDao]

    (anagraficaMock.getDataframe _).when(sqlContext).returns(in.getAnagrafica)
    (perimetroMock.getDataframe _).when(sqlContext).returns(in.getMisureInPerimetro)
    (misureMock.getDataframe _).when(sqlContext).returns(in.getMeasures)
    (rcuGasMassivoMock.getDataframe _).when(sqlContext).returns(in.getRCUGasMassivo.get)

    (_hdaoMock.getAnagraficaHDao _).when().returns(anagraficaMock)
    (_hdaoMock.getPerimetroPdrHDao _).when().returns(perimetroMock)
    (_hdaoMock.getCalcoloConsumiSbgHDao _).when().returns(misureMock)
    (_hdaoMock.getRCUGasMassivoPHDao _).when().returns(rcuGasMassivoMock)

    var row: Array[Row] = Array()

    val args: Array[String] = Array("2021/03/08", "61", "60", "false", "true")

    val result = CalcoloCtcFlow(args, _hdaoMock, executionId).run().cache()
    checkIfExistsPdr(result, ClgPdrCapacitaSchema.t_codice_pdr, "PDR3", false)

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR1"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR1"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "311.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "3421.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR2"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR2"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR4"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR4"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "100.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "1000.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR5"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDR5"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "28.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc2Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRX"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRX"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "100.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "1000.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRM"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRM"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDRF"))
      .select(cols.head, cols.tail: _*).take(1)
    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr_capacita_tmp)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pdr)), "IDPDRF"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_origine)), "spark-job-calcolo-capacita"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_processo_origine)), processoLabelConstant),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_id_pratica)), "null"), Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_rif)), "2021-03-08 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_inizio)), "2021-04-01 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_execution_id)), executionId),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_errore_agg_rcu)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_agg_rcu_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_stato)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_aggiornamento)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2700.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2020-12-31 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_mese)), "3"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_anno)), "2021"))

    result.unpersist()
  }

  test("testCaseA2 CHECK ALL PATH:" +
    " PDR1 KO WARN_CONT_FORM" +
    " PDR2 OK WARN MISURE " +
    " PDR3 OK WARN TRATTAMENTO perchè TOT>Z>0" +
    " PDR4 OK WARN TRATTAMENTO perchè TOT=Z" +
    " PDR5 OK Z=0 TIP CALC 1" +
    " PDR6 OK WARN CONT MIS Z=0 TIP CALC 2 o TIP CAL 3")
  {
    val in = CreatorFactory.getTestCreator(CreatorFactory.testCasea2)

    val _hdaoMock = stub[CalcoloCtcFlowInputHDao]
    val anagraficaMock = stub[AnagraficaHDao]
    val perimetroMock = stub[PerimetroPdrHDao]
    val misureMock = stub[CalcoloConsumiSbgHDao]
    val rcuGasMassivoMock = stub[RCUGasMassivoPHDao]

    (anagraficaMock.getDataframe _).when(sqlContext).returns(in.getAnagrafica)
    (perimetroMock.getDataframe _).when(sqlContext).returns(in.getMisureInPerimetro)
    (misureMock.getDataframe _).when(sqlContext).returns(in.getMeasures)
    (rcuGasMassivoMock.getDataframe _).when(sqlContext).returns(in.getRCUGasMassivo.get)

    (_hdaoMock.getAnagraficaHDao _).when().returns(anagraficaMock)
    (_hdaoMock.getPerimetroPdrHDao _).when().returns(perimetroMock)
    (_hdaoMock.getCalcoloConsumiSbgHDao _).when().returns(misureMock)
    (_hdaoMock.getRCUGasMassivoPHDao _).when().returns(rcuGasMassivoMock)

    var row: Array[Row] = Array()

    val args: Array[String] = Array("2021/03/08", "3", "30", "false", "true")

    val result = CalcoloCtcFlow(args, _hdaoMock, executionId).run().cache()

    result.show(100,false)

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR1"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_calcolo)), OK),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_code_desc)), WARN_CONT_FORN),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "200.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2000.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-01-30 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR2"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_calcolo)), OK),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_code_desc)), WARN_MISURE),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "200.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2000.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-01-30 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR3"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_calcolo)), OK),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_code_desc)), WARN_TRATTAMENTO),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "200.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2000.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-01-30 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR4"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_calcolo)), OK),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_code_desc)), WARN_TRATTAMENTO),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "200.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "2000.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-28 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-01-30 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc3Value))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR5"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_calcolo)), OK),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_code_desc)), "null"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "27.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "270.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-27 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-25 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc1Value))

    row = result.filter(col(ClgPdrCapacitaSchema.t_codice_pdr) === lit("PDR6"))
      .select(cols.head, cols.tail: _*).take(1)

    checksValues(
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_calcolo)), OK),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_esito_code_desc)), WARN_CONT_MIS),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_pcm)), "1000.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.n_ctc)), "10000.0"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_a)), "2021-02-27 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.d_data_da)), "2021-02-21 00:00:00"),
      Tuple2(getValueCalculatedOpti(row, mapping(ClgPdrCapacitaSchema.t_tipo_calcolo)), typeCalc2Value))


    result.unpersist()
  }


}

