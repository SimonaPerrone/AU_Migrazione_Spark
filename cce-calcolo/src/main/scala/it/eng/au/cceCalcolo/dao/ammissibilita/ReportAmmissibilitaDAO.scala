package it.eng.au.cceCalcolo.dao.ammissibilita

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lpad, split}
import org.apache.spark.sql.types.IntegerType

trait ReportAmmissibilitaDAO extends Dao {

  def getReportAmmissibilita: DataFrame = {
    Environment.spark.read.parquet(tablePath++"/bloccante=S/ammissibilita=N")
      .withColumn("annomese", split(col("nome_file"),"_")(2))
      .withColumn("annomese", col("annomese").cast(IntegerType))
      .withColumn("annomesegiornodir", concat(col("anno"), lpad(col("mese"),2,"0"), lpad(col("giorno"), 2, "0")))
  }

}
