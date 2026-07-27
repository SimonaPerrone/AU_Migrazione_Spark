package it.sferanet.au.model.prestazionale

import it.sferanet.au.controller.visitor._
import it.sferanet.au.model.Flow.ServiceName
import it.sferanet.au.model.flowTypes.Prestazionale

import java.util.Date

case class Im1Pre(service: String, //nomeFlusso
                  pdr: String, //cod_prd
                  date: Option[Date], //data
                  pivaDistr: Option[String], //pivaDistr
                  pivaUtente: Option[String], //pivaUtente
                  override val readType: Option[Char], //tipoMisura
                  measure: Option[Double], //letContatore
                  converted: Option[Double], //letMisuratore
                  serialNumberMis: Option[String], //matricola misuratore
                  serialNumberConv: Option[String], //matricola convertitore
                  override val coefCorr: Option[Double],
                  cau_int_mis: Option[Int],
                  cau_int_cor: Option[Int],
                  local_file: Option[String],
                  d_caricamento: Option[Date],
                  override val ammissibilita: Option[String],
                  isNewRoute: Boolean,
                  override val fileRettifica: Option[String] = None
                 ) extends Prestazionale {

  override def accept(visitor: IFlowVisitor): Unit = {
    visitor.visit(this)
  }

  override def accept[TReturnValue](visitor: IFlowWithReturnVisitor[TReturnValue]): TReturnValue = {
    visitor.visit(this)
  }
}

object Im1Pre {
  val serviceName: String = getClass.getSimpleNameUpperCase
}
