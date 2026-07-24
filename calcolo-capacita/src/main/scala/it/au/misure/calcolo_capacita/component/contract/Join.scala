package it.au.misure.calcolo_capacita.component.contract

import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.DataFrame

trait Join {


  def run(dataFrame1: DataFrame, dataFrame2: DataFrame)(implicit args: Args): DataFrame = {
    val c = calculation(dataFrame1, dataFrame2)
    c.selectExpr(getFieldsAfterJoin: _*)
  }

  protected def calculation(dataFrame1: DataFrame, dataFrame2: DataFrame)(implicit args: Args): DataFrame

  protected def getFieldsAfterJoin: List[String]

}
