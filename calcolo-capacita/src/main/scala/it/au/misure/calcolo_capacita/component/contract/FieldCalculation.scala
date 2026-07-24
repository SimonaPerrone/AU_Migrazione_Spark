package it.au.misure.calcolo_capacita.component.contract

import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.DataFrame

trait FieldCalculation {

  private def getFieldsToReturn: List[String] = getSchemaPreCalculation ::: List(getFieldCalculated)

  def run(dataFrame: DataFrame)(implicit args: Args): DataFrame = {
    calculation(dataFrame.selectExpr(getSchemaPreCalculation:_*)).selectExpr(getFieldsToReturn: _*)
  }

  protected def calculation(dataFrame: DataFrame)(implicit args: Args): DataFrame

  protected def getFieldCalculated: String

  protected def getSchemaPreCalculation: List[String]

}
