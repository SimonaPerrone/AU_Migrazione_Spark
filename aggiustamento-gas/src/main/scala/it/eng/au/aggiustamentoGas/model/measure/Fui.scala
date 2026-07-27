package it.eng.au.aggiustamentoGas.model.measure

import it.eng.au.aggiustamentoGas.model.measure.measureTypes.MisuraFlow
import org.joda.time.DateTime

case class Fui(service: String, //flusso
               pdr: String, //cod_pdr
               date: Option[DateTime], //date
               override val readType: Option[Char], //tipo_lettura
               measure: Option[Double], //Dato Misura 1
               converted: Option[Double], //Dato Misura 2
               serialNumberMis: Option[String], //matricola misuratore
               serialNumberConv: Option[String], //matricola convertitore
               pivaDistr: Option[String],
               pivaUtente: Option[String],
               localFile: Option[String],
               dataCaricamento: Option[DateTime],
               override val isCorrected: Boolean = false
              ) extends MisuraFlow
