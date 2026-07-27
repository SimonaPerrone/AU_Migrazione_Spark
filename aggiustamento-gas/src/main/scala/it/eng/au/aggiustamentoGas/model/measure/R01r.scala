package it.eng.au.aggiustamentoGas.model.measure

import it.eng.au.aggiustamentoGas.model.measure.measureTypes.RettificaFlow
import org.joda.time.DateTime

case class R01r(
                 service: String, //flusso
                 pdr: String, //cod_pdr
                 date: Option[DateTime], //date
                 measure: Option[Double], //Dato Misura 1
                 converted: Option[Double], //Dato Misura 2
                 serialNumberMis: Option[String], //matricola misuratore
                 serialNumberConv: Option[String], //matricola convertitore
                 pivaDistr: Option[String],
                 pivaUtente: Option[String],
                 localFile: Option[String],
                 dataCaricamento: Option[DateTime],
                 override val motivation: Option[Int],
                 override val isCorrected: Boolean = false
               ) extends RettificaFlow
