package it.eng.au.aggregatoreConsumiSbg.controller.traits

import it.eng.au.aggregatoreConsumiCommon.controller.traits.DtgTrait
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

trait DtgTraitSbg extends DtgTrait {
  /** [AU-603] Per ora non ci hanno richiesto di implementare il filtro anche per SBG */
  override def specificFilterForIncoerentiGdm: Column = lit(true)
}