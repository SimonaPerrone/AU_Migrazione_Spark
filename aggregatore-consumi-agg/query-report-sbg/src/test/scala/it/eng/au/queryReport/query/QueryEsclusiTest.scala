package it.eng.au.queryReport.query

import it.eng.au.aggregatoreConsumiCommon.schema.DailyConsumptionAggSchema
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import it.eng.au.queryReport.EnvironmentSparkTest
import it.eng.au.queryReport.query.esclusi.QueryDettaglioEsclusi
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DecimalType

class QueryEsclusiTest extends EnvironmentSparkTest {
  def testGetQuery(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dailyConsumptionDF = Environment.sparkContext.parallelize(
      (1 to 31)
        .map(i => (6, true, "N", "202001", "000PDR", "000DIST", "000UDD", "000UDB", "notSNam", "10238291008", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3"))
        ++
        List(
          (0, true, "N", "202001", "001PDR", "000DIST", "000UDD", "000UDB", "notSNam", "10238291008", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3"),
          (0, false, null, "202001", "000PDR", "000DIST", "000UDD", "000UDB", "notSNam", "10238291008", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", false, "sm3")
        ) ++
        (1 to 31)
          .map(_ => (0, true, "N", "202003", "002PDR", "000DIST", "000UDD", "000UDB", "notSNam", "10238291008", "Y", "000REMI", 0.1, 1.0, "000IDCL", "CODPROF_0", "U", "T", "S_AGG_1", true, "sm3")
          )
    ).toDF(
      DailyConsumptionAggSchema.errorCode,
      DailyConsumptionAggSchema.isValid,
      DailyConsumptionAggSchema.treatment,
      DailyConsumptionAggSchema.annoMese,
      DailyConsumptionAggSchema.pdr,
      DailyConsumptionAggSchema.pivaDistr,
      DailyConsumptionAggSchema.pivaUdd,
      DailyConsumptionAggSchema.pivaUdb,
      DailyConsumptionAggSchema.pivaIt,
      DailyConsumptionAggSchema.pivaRdb,
      DailyConsumptionAggSchema.dtg,
      DailyConsumptionAggSchema.codRemi,
      DailyConsumptionAggSchema.ca,
      DailyConsumptionAggSchema.valuef3,
      DailyConsumptionAggSchema.idRegClim,
      DailyConsumptionAggSchema.codProfStd,
      DailyConsumptionAggSchema.tipoCliente,
      DailyConsumptionAggSchema.causale,
      DailyConsumptionAggSchema.session,
      DailyConsumptionAggSchema.forcedExclusion,
      DailyConsumptionAggSchema.unitMisPrel
    )
      .withColumn(DailyConsumptionAggSchema.ca, col(DailyConsumptionAggSchema.ca).cast(DecimalType(12, 1)))

    val aggregatoDf = QueryDettaglioEsclusi.getQueryDF(dailyConsumptionDF)
    aggregatoDf.show(10, truncate = false)
    aggregatoDf.printSchema()
  }
}