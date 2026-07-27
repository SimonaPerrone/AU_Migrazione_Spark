package it.sferanet.au.model

import it.sferanet.au.utilities.DateUtils

import java.text.SimpleDateFormat
import java.util.Date

case class RcuGasProfilo(
                          n_id_var_profilo: String,
                          n_id_pdr: String,
                          t_anno: Int,
                          t_cod_profilo: String,
                          t_cod_cat_uso: String,
                          t_cod_classe_prelievo: String,
                          d_data_inizio: Option[Date],
                          d_data_fine: Option[Date]
                        ) extends Serializable {

  def checkFormula2(): Boolean = {
    val tCodProfOpt: Option[String] = Option(t_cod_profilo)
    tCodProfOpt.exists(cod => cod.startsWith("C2") || cod.startsWith("C4") || cod.startsWith("T1"))
  }
}

object RcuGasProfilo {
  def getRecordAtDate(iterable: Iterable[RcuGasProfilo], date: Date): Option[RcuGasProfilo] = {
    val lowerDate = DateUtils.getDateWithoutTime(new SimpleDateFormat("dd/MM/yyyy").parse("01/01/1900"))
    val upperDate = DateUtils.getDateWithoutTime(new SimpleDateFormat("dd/MM/yyyy").parse("01/01/2900"))
    val dateNoTime = DateUtils.getDateWithoutTime(date)

    iterable.find(r => {
      val nvlDataFineNoTime = DateUtils.getDateWithoutTime(r.d_data_fine.getOrElse(upperDate))
      val nvlDataInizioNoTime = DateUtils.getDateWithoutTime(r.d_data_inizio.getOrElse(lowerDate))

      (dateNoTime.before(nvlDataFineNoTime) || dateNoTime.equals(nvlDataFineNoTime)) &&
        (dateNoTime.equals(nvlDataInizioNoTime) || dateNoTime.after(nvlDataInizioNoTime))
    })
  }
}

