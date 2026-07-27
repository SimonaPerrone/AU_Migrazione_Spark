package it.eng.au.aggregatoreConsumiSbg.controller.traits

import it.eng.au.aggregatoreConsumiCommon.controller.traits.AggregatorTrait
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

trait AggregatorTraitSbg extends AggregatorTrait {
  /** [AU-603] Per ora non ci hanno richiesto di implementare il filtro anche per SBG */
  override def specificFilterForIncoerentiGdm: Column = lit(true)
}