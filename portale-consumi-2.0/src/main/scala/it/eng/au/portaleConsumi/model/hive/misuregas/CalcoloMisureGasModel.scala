package it.eng.au.portaleConsumi.model.hive.misuregas

import java.sql.Timestamp

case class CalcoloMisureGasModel(
                                  processo: String = null, //3M, 33M
                                  data_calcolo: String = null,
                                  ts_esecuzione: Timestamp = null
                                )
