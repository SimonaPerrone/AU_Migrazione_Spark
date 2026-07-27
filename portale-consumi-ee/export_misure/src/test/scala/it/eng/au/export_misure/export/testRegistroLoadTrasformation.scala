package it.eng.au.export_misure.`export`

import it.eng.au.export_misure.EnvironmentSparkTest
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure.{etlStage3M2ProposedModel, registroLoadModel}
import it.eng.au.portale_consumi_ee.trasformations.registroLoadTrasformation

class testRegistroLoadTrasformation extends EnvironmentSparkTest {

  val spark = EnvironmentMisure.getSpark

  import spark.implicits._

  def testupdateRegistroLoad() = {

    val etlStageDS = Seq(etlStage3M2ProposedModel(n_id_fornitura ="1" ,competenza_consumi = 202501),
    etlStage3M2ProposedModel(n_id_fornitura ="2" ,competenza_consumi = 202501),
    etlStage3M2ProposedModel(n_id_fornitura ="3" ,competenza_consumi = 202501),
      etlStage3M2ProposedModel(n_id_fornitura ="2" ,competenza_consumi = 202502),
      etlStage3M2ProposedModel(n_id_fornitura ="3" ,competenza_consumi = 202502),
      etlStage3M2ProposedModel(n_id_fornitura ="3" ,competenza_consumi = 202503)
    ).toDS()

    val registroLoadReaded = Seq(registroLoadModel()
    ).toDS()

    val output = registroLoadTrasformation.updateRegistroLoad(etlStageDS,false)

    output.show()
  }
}
