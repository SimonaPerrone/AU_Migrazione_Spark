package it.eng.au.queryReport.schema

import it.eng.au.aggregatoreConsumiCommon.schema.SchemaEnum

object ValidatedFlowsSchema extends SchemaEnum {
  val
  service,
  pdr,
  date,
  measure,
  converted,
  serialnumbermis,
  serialnumberconv,
  localfile,
  readtype,
  motivation,
  treatment,
  ncoeffcor,
  iscorrected,
  cauintmis,
  cauintcorr,
  executionid
  = Value
}
