package it.au.misure.calcolo_capacita.component.contract

import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.DataFrame

trait FieldCalculation4 {

  private def getFieldsToReturn: List[String] = getSchemaPreCalculation ::: List(getFieldCalculated1,getFieldCalculated2,getFieldCalculated3,getFieldCalculated4)

  def run(dataFrame: DataFrame)(implicit args: Args): DataFrame = {
    calculation(dataFrame.selectExpr(getSchemaPreCalculation:_*)).selectExpr(getFieldsToReturn: _*)
  }

  protected def calculation(dataFrame: DataFrame)(implicit args: Args): DataFrame

  protected def getFieldCalculated1: String
  protected def getFieldCalculated2: String
  protected def getFieldCalculated3: String
  protected def getFieldCalculated4: String

  protected def getSchemaPreCalculation: List[String]

}
