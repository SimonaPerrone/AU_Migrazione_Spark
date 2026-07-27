package it.eng.au.aggiustamentoGas.model.agg

import org.joda.time.DateTime

/** Modella i dati riguardanti le imprese di trasporto */
case class Conn2(
                  tRemi: String,
                  nIdAziendaIt: String,
                  tPivaIt: String,
                  dDataInizioAgg: DateTime,
                  dDataFineAgg: DateTime
                )
