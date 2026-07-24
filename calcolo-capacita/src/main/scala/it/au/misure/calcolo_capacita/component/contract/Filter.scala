package it.au.misure.calcolo_capacita.component.contract

import it.au.misure.calcolo_capacita.component.utility.check.Args
import org.apache.spark.sql.{Column, DataFrame}

trait Filter {

  def run(dataFrame: DataFrame)(implicit args: Args): DataFrame = {
    dataFrame.filter(getColumnCondition)
  }

  protected def getColumnCondition: Column

}
