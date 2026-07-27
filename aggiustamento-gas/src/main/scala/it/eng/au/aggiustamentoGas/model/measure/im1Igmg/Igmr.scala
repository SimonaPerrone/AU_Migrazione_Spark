package it.eng.au.aggiustamentoGas.model.measure.im1Igmg

import it.eng.au.aggiustamentoGas.model.measure.Flow
import it.eng.au.aggiustamentoGas.model.measure.measureTypes.RettificaFlow
import org.joda.time.DateTime

case class Igmr (service: String, //nomeFlusso
                 pdr: String, //cod_prd
                 date: Option[DateTime], //data
                 measure: Option[Double] = None, //letContatore
                 converted: Option[Double] = None, //letMisuratore
                 serialNumberMis: Option[String] = None, //matricola misuratore
                 serialNumberConv: Option[String] = None, //matricola convertitore
                 override val coefCorr: Option[Double] = None,
                 override val motivation: Option[Int] = None,
                 cau_int_mis: Option[Int],
                 cau_int_cor: Option[Int],
                 localFile: Option[String],
                 pivaDistr: Option[String],
                 pivaUtente: Option[String],
                 dataCaricamento: Option[DateTime],
                 pre: IgmrPre,
                 post: IgmrPost,
                 override var sameDayFlow: Option[Flow] = None,
                 override val isCorrected: Boolean = false
               ) extends Im1Igmg with RettificaFlow
