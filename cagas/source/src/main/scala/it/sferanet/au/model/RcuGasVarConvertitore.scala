package it.sferanet.au.model

import java.util.Date

case class RcuGasVarConvertitore(
                              startDate: Date,
                              endDate: Date,
                              n_id_pdr: String,
                              t_pre_conv: String,
                              n_num_cifre_convertitore: Option[Int]
                            ) extends Serializable {
}

object RcuGasVarConvertitore {
  implicit class RcuGasVarConvOps(val rcus: Iterable[RcuGasVarConvertitore]) extends AnyVal {
    def getByDate(date: Date): RcuGasVarConvertitore = {
      val filterRcus = rcus.filter(v => (v.startDate == date || v.startDate.before(date)) && (v.endDate == date || v.endDate.after(date)))
      if (filterRcus.isEmpty) null else filterRcus.max(orderingRcuMassivoTechEntries) // prendo singola entries definita con un max secondo uno specifico ordering
    }
  }

  val orderingRcuMassivoTechEntries: Ordering[RcuGasVarConvertitore] = new Ordering[RcuGasVarConvertitore] {
    override def compare(x: RcuGasVarConvertitore, y: RcuGasVarConvertitore): Int = {
      val comparesRules = List(
        x.endDate.compareTo(y.endDate),
        x.startDate.compareTo(y.startDate)
      )
      comparesRules.find(_ != 0).getOrElse(0)
    }
  }

}