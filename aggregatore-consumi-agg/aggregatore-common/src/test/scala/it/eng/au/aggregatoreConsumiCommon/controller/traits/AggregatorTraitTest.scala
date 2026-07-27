package it.eng.au.aggregatoreConsumiCommon.controller.traits

import it.eng.au.aggregatoreConsumiCommon.EnvironmentSparkTest
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggIncoerentiSchema, DailyConsumptionAggSchema, DailyConsumptionInputProcessSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit
import org.junit.Assert

import java.sql.Timestamp
import java.time.LocalDateTime
import scala.collection.immutable.ListMap

class AggregatorTraitTest extends EnvironmentSparkTest {
  def testGetAggregato(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val df = List(
      ("pdr1", "udd1", "udb1", "distr1", "it1", "rdb1", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "Y"),
      ("pdr1", "udd1", "udb1", "distr1", "it1", "rdb1", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "Y"),
      ("pdr1", "udd1", "udb1", "distr1", "it1", "rdb1", "202105", Timestamp.valueOf("2021-05-15 00:00:00"), 1, 1.0, 10, true, null),
      ("pdr1", "udd1", "udb1", "distr1", "it1", "rdb1", "202104", Timestamp.valueOf("2021-04-10 00:00:00"), 1, 1.0, 10, true, "Y"),
      ("pdr2", "udd1", "udb1", "distr1", "it1", "rdb1", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, null),
      ("pdr2", "udd1", "udb1", "distr1", "it1", "rdb1", "202105", Timestamp.valueOf("2021-05-10 00:00:00"), 1, 1.0, 10, true, "Y")
    ).toDF(
      DailyConsumptionAggSchema.pdr,
      DailyConsumptionAggSchema.pivaUdd,
      DailyConsumptionAggSchema.pivaUdb,
      DailyConsumptionAggSchema.pivaDistr,
      DailyConsumptionAggSchema.pivaIt,
      DailyConsumptionAggSchema.pivaRdb,
      DailyConsumptionAggSchema.annoMese,
      DailyConsumptionAggSchema.date,
      DailyConsumptionAggSchema.value,
      DailyConsumptionAggSchema.valuef3,
      DailyConsumptionAggSchema.errorCode,
      DailyConsumptionAggSchema.isValid,
      DailyConsumptionAggSchema.treatment
    ).withColumn(DailyConsumptionAggSchema.dtg, lit("notnull"))
      .withColumn(DailyConsumptionAggSchema.codRemi, lit("notnull"))
      .withColumn(DailyConsumptionAggSchema.codProfStd, lit("notnull"))
      .withColumn(DailyConsumptionAggSchema.tipoCliente, lit("notnull"))
      .withColumn(DailyConsumptionAggSchema.unitMisPrel, lit("notnull"))
      .withColumn(DailyConsumptionAggSchema.forcedExclusion, lit(null))
      .withColumn(DailyConsumptionAggSchema.session, lit("AGG_S1_PRE"))
      .withColumn(DailyConsumptionAggSchema.classeMisuratore, lit("G4"))
      .withColumn(DailyConsumptionAggSchema.coefficient, lit(1.0))
      .withColumn(DailyConsumptionInputProcessSchema.isPdrAnomalousGDM, lit(false))
      .withColumn(DailyConsumptionInputProcessSchema.isDayAnomalous, lit(false))
      .withColumn("esclusiFlag", lit(false))
      .withColumn("incoerentiFlag", lit(false))

    AggregatorTraitTest.run(df)


  }
}

object AggregatorTraitTest extends AggregatorTrait {
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DailyConsumptionAggSchema.pdr.toString -> "cod_pdr",
    DailyConsumptionAggSchema.pivaUdd.toString -> "piva_udd",
    DailyConsumptionAggSchema.treatment.toString -> "trattamento"
  )
  override val keyFields: List[String] = List("piva_udd")
  override val mainPiva: String = keyFields.head
  override val baseNumber: String = "1"

  override def fileSpecificFilterExpression: Column = lit(true)

  override val csvFields: List[String] = List(dataValColName) ::: aggregatoColumns.values.toList ::: (1 to 31).map(pivotPrefix + _).toList

}
