package it.sferanet.au.model

import java.util.Date

case class RcuGasMassivoTech(
                              startDate: Date,
                              endDate: Date,
                              t_codice_pdr: String,
                              n_coeff_correzione: Option[Double],
                              t_misuratore_integrato: Option[String],
                              t_pre_conv: Option[String],
                              n_num_cifre_misuratore: Option[Int],
                              n_num_cifre_convertitore: Option[Int]
                            ) extends Serializable {
}

object RcuGasMassivoTech {
  implicit class RcuGasTechOps(val rcus: Iterable[RcuGasMassivoTech]) extends AnyVal {
    def getByDate(date: Date): RcuGasMassivoTech = {
      val filterRcus = rcus.filter(v => (v.startDate == date || v.startDate.before(date)) && (v.endDate == date || v.endDate.after(date)))
      if (filterRcus.isEmpty) null else filterRcus.max(orderingRcuMassivoTechEntries) // prendo singola entries definita con un max secondo uno specifico ordering
    }
  }

  // ordinamento definito sulla base di data_fine_tech e data_inizio_tech (endDate e startDate sono sicuramente valorizzati != null in quanto gestito correttamente in fase di lettura della RcuGasTechTable)
  val orderingRcuMassivoTechEntries: Ordering[RcuGasMassivoTech] = new Ordering[RcuGasMassivoTech] {
    override def compare(x: RcuGasMassivoTech, y: RcuGasMassivoTech): Int = {
      val comparesRules = List(
        x.endDate.compareTo(y.endDate),
        x.startDate.compareTo(y.startDate)
      )
      comparesRules.find(_ != 0).getOrElse(0)
    }
  }

}