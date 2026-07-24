package it.au.misure.calcolo_capacita.component.implementation.filter

import it.au.misure.calcolo_capacita.component.contract.Filter
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.PATH$OK$NONPRESENTI
import it.au.misure.calcolo_capacita.component.utility.CalculatedField.PATH
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

object FilterPathOk$NonPresenti extends Filter {

  override protected def getColumnCondition: Column = col(PATH) === lit(PATH$OK$NONPRESENTI)
}
