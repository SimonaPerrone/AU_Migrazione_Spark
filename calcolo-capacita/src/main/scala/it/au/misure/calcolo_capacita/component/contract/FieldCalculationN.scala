package it.au.misure.calcolo_capacita.component.contract

import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.DataFrame

trait FieldCalculationN {

  def run(dataFrame: DataFrame)(implicit args: Args): DataFrame = {
    calculation(dataFrame.selectExpr(getSchemaPreCalculation:_*)).selectExpr(getFieldCalculated: _*)
  }

  protected def calculation(dataFrame: DataFrame)(implicit args: Args): DataFrame

  protected def getFieldCalculated: List[String]

  protected def getSchemaPreCalculation: List[String]

}
