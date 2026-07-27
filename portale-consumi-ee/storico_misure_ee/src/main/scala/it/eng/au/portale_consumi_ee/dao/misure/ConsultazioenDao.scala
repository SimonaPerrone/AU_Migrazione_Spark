package it.eng.au.portale_consumi_ee.dao.misure

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.dao.HiveMisureDao
import it.eng.au.portale_consumi_ee.model.misure.{ConsultazioneModel, misureStoricF2ErcEriModel}
import it.eng.au.portale_consumi_ee.schema.misure.{ConsultazioneSchema, misureStoricF2ErcEriSchema}
import it.eng.au.portale_consumi_ee.utility.setting.PropertyUtility

class ConsultazioenDao extends HiveMisureDao[ConsultazioneModel]{
  override val tableName: String = PropertyUtility.consultazione
  override val schema: SchemaEnum = ConsultazioneSchema

}