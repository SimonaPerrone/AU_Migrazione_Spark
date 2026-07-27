package it.eng.au.ERP.flow.IP

import it.eng.au.ERP.EnvironmentSparkTest
import it.eng.au.ERP.model.rcu.RcuAziendaPModel
import it.eng.au.ERP.schema.au.vAggreagazioneMisureIPSchema
import it.eng.au.ERP.schema.erp.erpAggregatoIPOSchema
import it.eng.au.ERP.trasformations.IP.CalcoloPrelevatoPuntiPrelievoOrariIPTrasformation
import it.eng.au.ERP.utility.args.ERPArgsConfig
import it.eng.au.ERP.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import org.junit.Assert.assertEquals

class IPCalcoloPrelevatoPuntiPrelievoOrariTrasformationTest extends EnvironmentSparkTest {
  implicit val spark = Environment.getSpark

  import spark.implicits._

  private val quartHourlySourceColumns: List[String] = vAggreagazioneMisureIPSchema.values.toList
    .map(_.toString)
    .filter(name => name.startsWith("h") && name.contains("_q"))

  private val (part1QuarterColumns, part2QuarterColumns) = quartHourlySourceColumns.splitAt(47)

  private val part1BaseColumns = Seq(
    vAggreagazioneMisureIPSchema.t_pod.toString,
    vAggreagazioneMisureIPSchema.t_area.toString,
    vAggreagazioneMisureIPSchema.n_id_dis.toString,
    vAggreagazioneMisureIPSchema.n_id_udd.toString,
    vAggreagazioneMisureIPSchema.d_data.toString
  )

  private val part1Schema = StructType(
    (part1BaseColumns ++ part1QuarterColumns ++ Seq("joinModel")).map(StructField(_, StringType, nullable = true))
  )

  private val part2Schema = StructType(
    (Seq("joinModel") ++ part2QuarterColumns ++ Seq(vAggreagazioneMisureIPSchema.annomese.toString))
      .map(StructField(_, StringType, nullable = true))
  )

  private def buildPart1Row(tPod: String,
                            area: String,
                            nIdDis: String,
                            nIdUdd: String,
                            data: String,
                            joinModel: String): Row = {
    val baseValues = Seq(tPod, area, nIdDis, nIdUdd, data)
    val quarterValues = Seq.fill(part1QuarterColumns.size)("1")
    Row.fromSeq(baseValues ++ quarterValues ++ Seq(joinModel))
  }

  private def buildPart2Row(joinModel: String, annomese: String): Row = {
    val quarterValues = Seq.fill(part2QuarterColumns.size)("1")
    Row.fromSeq(Seq(joinModel) ++ quarterValues ++ Seq(annomese))
  }

  private def buildPart1DataFrame(rows: Seq[Row]): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), part1Schema)

  private def buildPart2DataFrame(rows: Seq[Row]): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), part2Schema)

  def testCalcoloPrelevatoPuntiPrelievoOrariIPTransformation(): Unit = {
    val part1Rows = Seq(
      buildPart1Row("pod1", "AREA1", "DIS123", "UDD001", "2025-07-03 00:00:00.0", "join1"),
      buildPart1Row("pod4", "AREA1", "DIS123", "UDD004", "2025-07-03 00:00:00.0", "join4"),
      buildPart1Row("pod2", "AREA1", "DIS123", "UDD002", "2025-07-03 00:00:00.0", "join2"),
      buildPart1Row("pod3", "AREA2", "DIS777", "UDD003", "2025-07-04 00:00:00.0", "join3")
    )

    val part2Rows = Seq(
      buildPart2Row("join1", "202507"),
      buildPart2Row("join4", "202507"),
      buildPart2Row("join2", "202507"),
      buildPart2Row("join3", "202507")
    )

    val vAggregazioneMisureIPJoin = buildPart1DataFrame(part1Rows)
      .join(buildPart2DataFrame(part2Rows), "joinModel")

    val aziende = Seq(
      RcuAziendaPModel(n_id_azienda = "DIS123", t_piva = "piva2", t_rag_soc = "rag_soc2"),
      RcuAziendaPModel(n_id_azienda = "DIS777", t_piva = "piva4", t_rag_soc = "rag_soc4")
    ).toDS()

    val podExcluded = List("pod2")
    val timestamp = 123456789L
    val args = ERPArgsConfig()
    val year = Some(2025)
    val month = Some(7)
    val annomese = Some("202507")
    val area = Some("AREA1")
    val singolaPiva = Some("piva2")

    val finalDf = CalcoloPrelevatoPuntiPrelievoOrariIPTrasformation.calcoloPrelevatoPuntiPrelievoOrariIP(
      vAggregazioneMisureIPJoin,
      aziende,
      args,
      podExcluded,
      timestamp,
      year,
      month,
      annomese,
      area,
      singolaPiva
    )

    val rows = finalDf.collect()
    assertEquals("Expected a single aggregated row for AREA1 and piva2", 1, rows.length)

    val row = rows.head
    assertEquals(3, row.getAs[Int](erpAggregatoIPOSchema.giorno))
    assertEquals(2025, row.getAs[Int](erpAggregatoIPOSchema.anno))
    assertEquals(7, row.getAs[Int](erpAggregatoIPOSchema.mese))
    assertEquals("AREA1", row.getAs[String](erpAggregatoIPOSchema.area))
    assertEquals("piva2", row.getAs[String](erpAggregatoIPOSchema.piva_distr))
    assertEquals("rag_soc2", row.getAs[String](erpAggregatoIPOSchema.rag_soc_distr))
    assertEquals(timestamp, row.getAs[Long](erpAggregatoIPOSchema.executionid))
    assertEquals(2.0, row.getAs[Double](erpAggregatoIPOSchema.q1), 0.0001)
    assertEquals(2.0, row.getAs[Double](erpAggregatoIPOSchema.q100), 0.0001)
    assertEquals(erpAggregatoIPOSchema.getValues.size, finalDf.columns.length)
  }
}
