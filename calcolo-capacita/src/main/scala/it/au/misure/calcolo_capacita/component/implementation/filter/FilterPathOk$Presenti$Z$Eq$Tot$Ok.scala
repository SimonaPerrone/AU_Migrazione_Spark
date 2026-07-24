package it.au.misure.calcolo_capacita.component.implementation.filter

import it.au.misure.calcolo_capacita.component.contract.Filter
import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.PATH$OK$PRESENTI$Z$EQ$TOT$OK
import it.au.misure.calcolo_capacita.component.utility.CalculatedField.PATH
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

object FilterPathOk$Presenti$Z$Eq$Tot$Ok extends Filter {

  override protected def getColumnCondition: Column = col(PATH) === lit(PATH$OK$PRESENTI$Z$EQ$TOT$OK)
}
