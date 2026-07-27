package it.eng.au.aggiustamentoGas.model.agg

object ErrorEnum extends Enumeration {
  val NO_ERROR_CODE: ErrorEnum.Value = Value(0)
  val NOT_CONSECUTIVE_DAYS_ERROR_CODE: ErrorEnum.Value = Value(1)
  val COD_PROF_STD_ERROR_CODE: ErrorEnum.Value = Value(2)
  val ID_REG_CLIM_ERROR_CODE: ErrorEnum.Value = Value(3)
  val PPROF_K_ERROR_CODE: ErrorEnum.Value = Value(4)
  val NON_MATCHING_SERIALS_ERROR_CODE: ErrorEnum.Value = Value(5)
  val TREATMENT_IS_N_ERROR_CODE: ErrorEnum.Value = Value(6)
  val NOT_IMPLEMENTED_SCENARIO_ERROR_CODE: ErrorEnum.Value = Value(7)
  val CA_NOT_PRESENT_ERROR_CODE: ErrorEnum.Value = Value(8)
  val MEASURE_NOT_PRESENT_ERROR_CODE: ErrorEnum.Value = Value(9)
  val PDR_WITHOUT_SEGMENTS_ERROR_CODE: ErrorEnum.Value = Value(10)
  val TREATMENT_IS_NULL_ERROR_CODE: ErrorEnum.Value = Value(11)
  val CONSUMPTION_IS_NEGATIVE_ERROR_CODE: ErrorEnum.Value = Value(12)
  val FURNITURE_INACTIVE_ERROR_CODE: ErrorEnum.Value = Value(13)
  val CONSUMPTION_UNDEFINED_ERROR_CODE: ErrorEnum.Value = Value(14)
  val CARRI_BOMBOLAI_ERROR_CODE: ErrorEnum.Value = Value(15)

  private val errorPriorityMap = Map(
    NO_ERROR_CODE -> 0,
    CONSUMPTION_UNDEFINED_ERROR_CODE -> 1,
    NOT_CONSECUTIVE_DAYS_ERROR_CODE -> 2,
    PDR_WITHOUT_SEGMENTS_ERROR_CODE -> 3,
    TREATMENT_IS_NULL_ERROR_CODE -> 4,
    CONSUMPTION_IS_NEGATIVE_ERROR_CODE -> 5,
    PPROF_K_ERROR_CODE -> 6,
    MEASURE_NOT_PRESENT_ERROR_CODE -> 7,
    NON_MATCHING_SERIALS_ERROR_CODE -> 8,
    TREATMENT_IS_N_ERROR_CODE -> 9,
    ID_REG_CLIM_ERROR_CODE -> 10,
    COD_PROF_STD_ERROR_CODE -> 11,
    CA_NOT_PRESENT_ERROR_CODE -> 12,
    NOT_IMPLEMENTED_SCENARIO_ERROR_CODE -> 13,
    CARRI_BOMBOLAI_ERROR_CODE -> 14,
    FURNITURE_INACTIVE_ERROR_CODE -> 15
  )

  def getMaxPriorityError(errors: Array[ErrorEnum.Value]): ErrorEnum.Value = errors.map(e => (e, errorPriorityMap(e))).maxBy(_._2)._1

}
