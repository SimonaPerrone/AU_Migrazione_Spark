package it.au.misure.calcolo_capacita.component.contract

import it.au.misure.calcolo_capacita.component.utility.CalculatedField.fieldsCalculatePcm
import org.apache.spark.sql.DataFrame

trait CalculationType {

  def getFieldsToReturn: List[String] = fieldsCalculatePcm

  def run(dataFrame: DataFrame): DataFrame = dataFrame

}
