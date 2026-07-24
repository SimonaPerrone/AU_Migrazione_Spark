package it.au.misure.calcolo_capacita.component.utility

import it.au.misure.calcolo_capacita.component.schema.{CalcoloConsumiSbgSchema, ClgPdrCapacitaSchema}

object CalculatedField {

  val annoMeseGiornoString = "annoMeseGiornoString"
  val annoMeseGiornoDate = "annoMeseGiornoDate"
  val lastDayOfMonth = "lastDayOfMonth"
  val isLastDayOfMonth = "isLastDayOfMonth"
  val firtDayOfMonth = "firtDayOfMonth"
  val isFirtDayOfMonth = "isFirtDayOfMonth"
  val toTake="toTake"
  val dayWithAtLeastXDayConsecutive="dayWithAtLeastXDayConsecutive"
  val caMax = "ca-max"
  val prev="prev"
  val diffCurrPrevDay="diffCurrPrevDay"
  val diffCurrPrevDayUpdate="diffCurrPrevDayUpdate"
  val isInfraMese1="isInfraMese1"
  val intensitàGapBeforeCurrent="intensitàGapBeforeCurrent"
  val numberDayConsecutiveAfterCurrentOnXWindow0="numberDayConsecutiveAfterCurrentOnXWindow0"
  val numberDayConsecutiveAfterCurrentOnXWindow="numberDayConsecutiveAfterCurrentOnXWindow"
  val progressiveDayTodate="progressive_day_todate"
  val numericDay = "numericDay"
  val dayWith0 = "day-with-0"
  val flagRcuGasMassivo="flagRcuGasMassivo"
  val flagAnagrafica="flagAnagrafica"
  val TOT = "TOT"
  val Ztemp = "Ztemp"
  val Z = "Z"
  val PATH = "PATH"
  val PATH_CHECK_FORNITURA = "PATH_CHECK_FORNITURA"
  val PATH_CHECK_MISURE = "PATH_CHECK_MISURE"
  val PATH_CHECK_TRATTAMENTO = "PATH_CHECK_TRATTAMENTO"
  val pdrAnagraficaCol = "pdrAnagraficaCol"

  val fieldsCalculatePcm = List[String](
    CalcoloConsumiSbgSchema.cod_pdr,
    ClgPdrCapacitaSchema.n_pcm,
    ClgPdrCapacitaSchema.t_tipo_calcolo,
    ClgPdrCapacitaSchema.d_data_da,
    ClgPdrCapacitaSchema.d_data_a)

}
