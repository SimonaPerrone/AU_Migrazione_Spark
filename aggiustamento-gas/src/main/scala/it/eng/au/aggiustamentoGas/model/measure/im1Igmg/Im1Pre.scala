package it.eng.au.aggiustamentoGas.model.measure.im1Igmg

import it.eng.au.aggiustamentoGas.model.measure.measureTypes.RettificaFlow
import org.joda.time.DateTime

case class Im1Pre(
                   service: String, //nomeFlusso
                   pdr: String, //cod_prd
                   date: Option[DateTime], //data
                   override val readType: Option[Char], //tipoMisura
                   measure: Option[Double], //letContatore
                   converted: Option[Double], //letMisuratore
                   serialNumberMis: Option[String], //matricola misuratore
                   serialNumberConv: Option[String], //matricola convertitore
                   pivaDistr: Option[String],
                   pivaUtente: Option[String],
                   coefCorr: Option[Double],
                   cau_int_mis: Option[Int],
                   cau_int_cor: Option[Int],
                   localFile: Option[String],
                   dataCaricamento: Option[DateTime],
                   override val isCorrected: Boolean = false,
                   override val correctionFlow: Option[RettificaFlow] = None
                 ) extends Pre {

}
