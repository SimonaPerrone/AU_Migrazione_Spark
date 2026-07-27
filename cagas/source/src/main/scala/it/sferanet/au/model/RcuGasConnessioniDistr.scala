package it.sferanet.au.model

import it.sferanet.au.utilities.DateUtils

import java.text.SimpleDateFormat
import java.util.Date

case class RcuGasConnessioniDistr(
                                   t_codice_pdr: String,
                                   n_id_pdr: String,
                                   n_id_remi: String,
                                   d_data_inizio_conn: Option[Date],
                                   d_data_fine_conn: Option[Date],
                                   t_remi: String
                                 ) extends Serializable {
}

object RcuGasConnessioniDistr {
  def getRecordAtDate(iterable: Iterable[RcuGasConnessioniDistr], date: Date): Option[RcuGasConnessioniDistr] = {
    val lowerDate = DateUtils.getDateWithoutTime(new SimpleDateFormat("dd/MM/yyyy").parse("01/01/1900"))
    val upperDate = DateUtils.getDateWithoutTime(new SimpleDateFormat("dd/MM/yyyy").parse("01/01/2900"))
    val dateNoTime = DateUtils.getDateWithoutTime(date)

    iterable.find(r => {
      val nvlDataFineNoTime = DateUtils.getDateWithoutTime(r.d_data_inizio_conn.getOrElse(upperDate))
      val nvlDataInizioNoTime = DateUtils.getDateWithoutTime(r.d_data_fine_conn.getOrElse(lowerDate))

      (dateNoTime.before(nvlDataFineNoTime) || dateNoTime.equals(nvlDataFineNoTime)) &&
        (dateNoTime.equals(nvlDataInizioNoTime) || dateNoTime.after(nvlDataInizioNoTime))
    })
  }
}

