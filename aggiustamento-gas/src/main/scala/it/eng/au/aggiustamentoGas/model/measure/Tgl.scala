package it.eng.au.aggiustamentoGas.model.measure

import it.eng.au.aggiustamentoGas.model.measure.measureTypes.MisuraFlow
import org.joda.time.DateTime

case class Tgl(
                service: String, //nomeFlusso
                pdr: String, //cod_prd
                date: Option[DateTime], //data
                override val readType: Option[Char], //tipoMisura
                override val isValid: Option[String] = None, //valDatoMens
                measure: Option[Double], //letTotPrel
                converted: Option[Double], //letTotConv
                serialNumberMis: Option[String], //matricola misuratore
                serialNumberConv: Option[String], //matricola convertitore
                pivaDistr: Option[String],
                pivaUtente: Option[String],
                localFile: Option[String],
                dataCaricamento: Option[DateTime],
                override val isCorrected: Boolean = false
              ) extends MisuraFlow
