package it.eng.au.aggiustamentoGas.model.measure

import it.eng.au.aggiustamentoGas.model.measure.measureTypes.MisuraFlow
import org.joda.time.DateTime

case class Swg1(
                 service: String, //nomeFlusso
                 flowCode: Option[String],
                 pdr: String, //cod_prd
                 date: Option[DateTime], //data
                 override val readType: Option[Char], //tipo_lettura
                 measure: Option[Double], //letTotPrel
                 converted: Option[Double], //letTotConv
                 serialNumberMis: Option[String], //matricola misuratore
                 serialNumberConv: Option[String], //matricola convertitore
                 localFile: Option[String],
                 pivaDistr: Option[String],
                 pivaUtente: Option[String],
                 dataCaricamento: Option[DateTime],
                 override val isCorrected: Boolean = false
               ) extends MisuraFlow
