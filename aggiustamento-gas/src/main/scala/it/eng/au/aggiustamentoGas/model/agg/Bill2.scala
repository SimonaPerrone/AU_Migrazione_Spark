package it.eng.au.aggiustamentoGas.model.agg

import org.joda.time.DateTime

/** Modella i dati riguardanti il processo di bilanciamento */
case class Bill2(
                  tCodicePdr: String,
                  nIdPdr: String,
                  nIdUdb: String,
                  nIdAziendaUdb: String,
                  tPivaUdb: String,
                  dDataInizioBil: DateTime,
                  dDataFineBil: DateTime
                )
