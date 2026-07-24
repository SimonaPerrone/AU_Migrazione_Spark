package it.au.misure.calcolo_capacita.component.utility.`implicit`


import org.apache.spark.sql.DataFrame

object DataframeUtility {

  implicit class DataframeUtility(df: DataFrame) {

    def extendColName(colName: String, extension: String): (DataFrame, String) = {
      val newName = f"${colName}${extension}"
      val newDf = df.withColumnRenamed(colName, newName)
      (newDf, newName)
    }
  }

}