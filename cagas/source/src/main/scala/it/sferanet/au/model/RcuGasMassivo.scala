package it.sferanet.au.model

import java.util.Date

case class RcuGasMassivo(
                          startDate: Date,
                          endDate: Date,
                          t_codice_pdr: String,
                          cat_uso: String,
                          @deprecated t_cod_profilo: String,
                          t_processo: String,
                          id_regione_climatica: Int,
                          t_comune_istat_pdr: String,
                          t_anno_termico: Int,
                          n_prelievo_annuo: String,
                          n_id_pdr: String
                        ) extends Serializable {

  def checkFormula2(): Boolean = {
    Seq("C2", "C4", "T1").contains(if (t_cod_profilo.size > 2) t_cod_profilo.substring(0, 2) else "")
  }
}

object RcuGasMassivo {

  implicit class RcuGasOps(val rcus: Iterable[RcuGasMassivo]) extends AnyVal {
    def getByDate(date: Date): RcuGasMassivo = {
      val filterRcus = rcus.filter(v => (v.startDate == date || v.startDate.before(date)) && (v.endDate == date || v.endDate.after(date)))
      if (filterRcus.isEmpty) null else filterRcus.head
    }
  }

}
