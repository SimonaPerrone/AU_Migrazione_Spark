package it.eng.au.aggiustamentoGas.model.measure

import it.eng.au.aggiustamentoGas.model.measure.measureTypes.RettificaFlow
import org.joda.time.DateTime

case class Rgl(
                service: String, //nomeFlusso
                pdr: String, //cod_prd
                date: Option[DateTime], //data
                measure: Option[Double], //letTotPrel
                converted: Option[Double], //letTotConv
                serialNumberMis: Option[String], //matricola misuratore
                serialNumberConv: Option[String], //matricola convertitore
                pivaDistr: Option[String],
                pivaUtente: Option[String],
                override val motivation: Option[Int], //mottRettLett
                localFile: Option[String],
                dataCaricamento: Option[DateTime],
                override val isCorrected: Boolean = false
              ) extends RettificaFlow
