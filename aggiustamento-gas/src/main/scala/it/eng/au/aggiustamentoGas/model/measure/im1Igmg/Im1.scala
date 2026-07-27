package it.eng.au.aggiustamentoGas.model.measure.im1Igmg

import it.eng.au.aggiustamentoGas.model.measure.Flow
import org.joda.time.DateTime

case class Im1(service: String, //nomeFlusso
               pdr: String, //cod_prd
               date: Option[DateTime], //data
               override val readType: Option[Char], //tipoMisura
               measure: Option[Double] = None, //letContatore
               converted: Option[Double] = None, //letMisuratore
               serialNumberMis: Option[String] = None, //matricola misuratore
               serialNumberConv: Option[String] = None, //matricola convertitore
               override val coefCorr: Option[Double] = None,
               cau_int_mis: Option[Int],
               cau_int_cor: Option[Int],
               pivaDistr: Option[String],
               pivaUtente: Option[String],
               localFile: Option[String],
               dataCaricamento: Option[DateTime],
               pre: Im1Pre,
               post: Im1Post,
               override var sameDayFlow: Option[Flow] = None,
               override val isCorrected: Boolean = false
              ) extends Im1Igmg
