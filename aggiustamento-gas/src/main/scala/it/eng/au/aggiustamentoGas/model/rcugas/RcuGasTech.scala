package it.eng.au.aggiustamentoGas.model.rcugas

import org.joda.time.DateTime

case class RcuGasTech(
                       nIdPdr: String,
                       gruppoMisInt: Option[String] = None, //t_misuratore_integrato
                       classeMisuratore: Option[String]= None, //t_classe_misuratore
                       nCoeffCorr: Option[Double] = None,
                       startDateTech: DateTime,
                       endDateTech: DateTime,
                       nCifreMis: Option[Int] = None
                     )
