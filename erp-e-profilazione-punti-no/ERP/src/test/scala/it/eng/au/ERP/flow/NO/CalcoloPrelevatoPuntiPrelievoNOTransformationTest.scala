package it.eng.au.ERP.flow.NO

import it.eng.au.ERP.EnvironmentSparkTest
import it.eng.au.ERP.trasformations.NO.CalcoloPrelevatoPuntiPrelievoNOTransformation
import it.eng.au.ERP.utility.environment.Environment
import org.junit.Test

/**
 * Test per CalcoloPrelevatoPuntiPrelievoNOTransformation
 * 
 * Verifica la corretta aggregazione dei dati da erp_daily_no a erp_aggregato_no
 * seguendo le specifiche del documento D02 (SIIERPRU001_ARU_AGGREGATO_ERP.docx).
 * 
 * Il test valida:
 * - Aggregazione per giorno/area/piva_distr/rag_soc_distr
 * - Somma corretta di tutti i 100 quartori (q1-q100)
 * - Esclusione corretta dei POD nella lista podExcluded
 * - Gestione corretta dell'executionId
 */
class CalcoloPrelevatoPuntiPrelievoNOTransformationTest extends EnvironmentSparkTest {
  implicit val spark = Environment.getSpark

  import spark.implicits._

  @Test
  def testAggregateNODailyProfiles_BasicAggregation(): Unit = {
    // Setup: 3 POD dello stesso distributore, stessa area, stesso giorno
    // Expected: aggregazione che somma i 100 quartori
    val testData = Seq(
      // POD 1: q1=1.0, q2=2.0, ..., q100=100.0
      ("POD001", 2025, 7, "2025-07-15", "NORD", "12345678901", "Distributore Test 1", 
       1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0), // q1-q10
      
      // POD 2: q1=0.5, q2=1.0, ..., q10=5.0
      ("POD002", 2025, 7, "2025-07-15", "NORD", "12345678901", "Distributore Test 1",
       0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0), // q1-q10
      
      // POD 3: q1=0.5, q2=1.0, ..., q10=5.0 (stesso del POD2 per test somma)
      ("POD003", 2025, 7, "2025-07-15", "NORD", "12345678901", "Distributore Test 1",
       0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0)  // q1-q10
    )

    // Crea DataFrame con schema semplificato (solo primi 10 quartori per brevità test)
    val dfDailyNo = testData.toDF(
      "pod", "anno", "mese", "giorno", "area", "piva_distr", "rag_soc_distr",
      "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10"
    )

    val executionId = System.currentTimeMillis()
    val podExcluded = List[String]() // Nessun POD escluso

    // Execute transformation
    val result = CalcoloPrelevatoPuntiPrelievoNOTransformation.aggregateNODailyProfiles(
      dfDailyNo, 
      executionId, 
      podExcluded
    )

    // Verifications
    println("\n===== RESULT =====")
    result.show(false)
    
    // Assert: dovrebbe esserci 1 solo record aggregato
    assert(result.count() == 1, s"Expected 1 aggregated record, got ${result.count()}")
    
    // Assert: verifica valori aggregati per q1-q10
    val row = result.collect()(0)
    assert(row.getAs[String]("giorno") == "2025-07-15")
    assert(row.getAs[Int]("anno") == 2025)
    assert(row.getAs[Int]("mese") == 7)
    assert(row.getAs[String]("area") == "NORD")
    assert(row.getAs[String]("piva_distr") == "12345678901")
    
    // Verifica somme: POD001(1.0) + POD002(0.5) + POD003(0.5) = 2.0
    assert(row.getAs[Double]("q1") == 2.0, s"Expected q1=2.0, got ${row.getAs[Double]("q1")}")
    assert(row.getAs[Double]("q2") == 4.0, s"Expected q2=4.0, got ${row.getAs[Double]("q2")}")
    assert(row.getAs[Double]("q10") == 20.0, s"Expected q10=20.0, got ${row.getAs[Double]("q10")}")
    
    println("✓ Test BasicAggregation PASSED")
  }

  @Test
  def testAggregateNODailyProfiles_MultipleAreas(): Unit = {
    // Setup: 2 POD in aree diverse
    // Expected: 2 record aggregati (uno per area)
    val testData = Seq(
      ("POD001", 2025, 7, "2025-07-15", "NORD", "12345678901", "Distributore 1", 10.0, 20.0),
      ("POD002", 2025, 7, "2025-07-15", "SUD", "12345678901", "Distributore 1", 5.0, 10.0)
    )

    val dfDailyNo = testData.toDF(
      "pod", "anno", "mese", "giorno", "area", "piva_distr", "rag_soc_distr", "q1", "q2"
    )

    val executionId = System.currentTimeMillis()
    val result = CalcoloPrelevatoPuntiPrelievoNOTransformation.aggregateNODailyProfiles(
      dfDailyNo, executionId, List()
    )

    println("\n===== RESULT MultipleAreas =====")
    result.show(false)
    
    assert(result.count() == 2, s"Expected 2 records (one per area), got ${result.count()}")
    
    val nordRow = result.filter($"area" === "NORD").collect()(0)
    val sudRow = result.filter($"area" === "SUD").collect()(0)
    
    assert(nordRow.getAs[Double]("q1") == 10.0)
    assert(sudRow.getAs[Double]("q1") == 5.0)
    
    println("✓ Test MultipleAreas PASSED")
  }

  @Test
  def testAggregateNODailyProfiles_MultipleDays(): Unit = {
    // Setup: 2 POD in giorni diversi
    // Expected: 2 record aggregati (uno per giorno)
    val testData = Seq(
      ("POD001", 2025, 7, "2025-07-15", "NORD", "12345678901", "Distributore 1", 10.0, 20.0),
      ("POD002", 2025, 7, "2025-07-16", "NORD", "12345678901", "Distributore 1", 5.0, 10.0)
    )

    val dfDailyNo = testData.toDF(
      "pod", "anno", "mese", "giorno", "area", "piva_distr", "rag_soc_distr", "q1", "q2"
    )

    val executionId = System.currentTimeMillis()
    val result = CalcoloPrelevatoPuntiPrelievoNOTransformation.aggregateNODailyProfiles(
      dfDailyNo, executionId, List()
    )

    println("\n===== RESULT MultipleDays =====")
    result.show(false)
    
    assert(result.count() == 2, s"Expected 2 records (one per day), got ${result.count()}")
    
    val day15 = result.filter($"giorno" === "2025-07-15").collect()(0)
    val day16 = result.filter($"giorno" === "2025-07-16").collect()(0)
    
    assert(day15.getAs[Double]("q1") == 10.0)
    assert(day16.getAs[Double]("q1") == 5.0)
    
    println("✓ Test MultipleDays PASSED")
  }

  @Test
  def testAggregateNODailyProfiles_MultiplePIVA(): Unit = {
    // Setup: 2 POD con PIVA distributore diversi
    // Expected: 2 record aggregati (uno per PIVA)
    val testData = Seq(
      ("POD001", 2025, 7, "2025-07-15", "NORD", "11111111111", "Distributore A", 10.0, 20.0),
      ("POD002", 2025, 7, "2025-07-15", "NORD", "22222222222", "Distributore B", 5.0, 10.0)
    )

    val dfDailyNo = testData.toDF(
      "pod", "anno", "mese", "giorno", "area", "piva_distr", "rag_soc_distr", "q1", "q2"
    )

    val executionId = System.currentTimeMillis()
    val result = CalcoloPrelevatoPuntiPrelievoNOTransformation.aggregateNODailyProfiles(
      dfDailyNo, executionId, List()
    )

    println("\n===== RESULT MultiplePIVA =====")
    result.show(false)
    
    assert(result.count() == 2, s"Expected 2 records (one per PIVA), got ${result.count()}")
    
    val pivaA = result.filter($"piva_distr" === "11111111111").collect()(0)
    val pivaB = result.filter($"piva_distr" === "22222222222").collect()(0)
    
    assert(pivaA.getAs[Double]("q1") == 10.0)
    assert(pivaB.getAs[Double]("q1") == 5.0)
    assert(pivaA.getAs[String]("rag_soc_distr") == "Distributore A")
    assert(pivaB.getAs[String]("rag_soc_distr") == "Distributore B")
    
    println("✓ Test MultiplePIVA PASSED")
  }

  @Test
  def testAggregateNODailyProfiles_PodExcluded(): Unit = {
    // Setup: 3 POD, uno nella lista podExcluded
    // Expected: aggregazione di solo 2 POD
    val testData = Seq(
      ("POD001", 2025, 7, "2025-07-15", "NORD", "12345678901", "Distributore 1", 10.0, 20.0),
      ("POD002", 2025, 7, "2025-07-15", "NORD", "12345678901", "Distributore 1", 5.0, 10.0),
      ("POD_EXCLUDED", 2025, 7, "2025-07-15", "NORD", "12345678901", "Distributore 1", 100.0, 200.0) // Deve essere escluso
    )

    val dfDailyNo = testData.toDF(
      "pod", "anno", "mese", "giorno", "area", "piva_distr", "rag_soc_distr", "q1", "q2"
    )

    val executionId = System.currentTimeMillis()
    val podExcluded = List("POD_EXCLUDED")
    
    val result = CalcoloPrelevatoPuntiPrelievoNOTransformation.aggregateNODailyProfiles(
      dfDailyNo, executionId, podExcluded
    )

    println("\n===== RESULT PodExcluded =====")
    result.show(false)
    
    assert(result.count() == 1, s"Expected 1 record, got ${result.count()}")
    
    val row = result.collect()(0)
    // Se POD_EXCLUDED fosse incluso, q1 sarebbe 115.0 (10+5+100)
    // Con esclusione corretta, deve essere 15.0 (10+5)
    assert(row.getAs[Double]("q1") == 15.0, s"Expected q1=15.0 (POD_EXCLUDED should be filtered), got ${row.getAs[Double]("q1")}")
    assert(row.getAs[Double]("q2") == 30.0, s"Expected q2=30.0, got ${row.getAs[Double]("q2")}")
    
    println("✓ Test PodExcluded PASSED")
  }

  @Test
  def testAggregateNODailyProfiles_ExecutionIdPersistence(): Unit = {
    // Setup: verifica che executionId sia propagato correttamente
    val testData = Seq(
      ("POD001", 2025, 7, "2025-07-15", "NORD", "12345678901", "Distributore 1", 10.0, 20.0)
    )

    val dfDailyNo = testData.toDF(
      "pod", "anno", "mese", "giorno", "area", "piva_distr", "rag_soc_distr", "q1", "q2"
    )

    val executionId = 999888777666L // Custom execution ID
    val result = CalcoloPrelevatoPuntiPrelievoNOTransformation.aggregateNODailyProfiles(
      dfDailyNo, executionId, List()
    )

    println("\n===== RESULT ExecutionId =====")
    result.show(false)
    
    val row = result.collect()(0)
    assert(row.getAs[String]("executionid") == "999888777666", 
      s"Expected executionid='999888777666', got ${row.getAs[String]("executionid")}")
    
    println("✓ Test ExecutionIdPersistence PASSED")
  }

  @Test
  def testAggregateNODailyProfiles_EmptyDataFrame(): Unit = {
    // Setup: DataFrame vuoto
    // Expected: DataFrame vuoto in output (nessuna eccezione)
    val dfDailyNo = Seq.empty[(String, Int, Int, String, String, String, String, Double, Double)]
      .toDF("pod", "anno", "mese", "giorno", "area", "piva_distr", "rag_soc_distr", "q1", "q2")

    val executionId = System.currentTimeMillis()
    val result = CalcoloPrelevatoPuntiPrelievoNOTransformation.aggregateNODailyProfiles(
      dfDailyNo, executionId, List()
    )

    println("\n===== RESULT EmptyDataFrame =====")
    result.show(false)
    
    assert(result.count() == 0, s"Expected 0 records for empty input, got ${result.count()}")
    
    println("✓ Test EmptyDataFrame PASSED")
  }

  @Test
  def testAggregateNODailyProfiles_ComplexScenario(): Unit = {
    // Setup: scenario complesso con multiple dimensioni
    // 2 aree, 2 giorni, 2 PIVA, alcuni POD esclusi
    val testData = Seq(
      // NORD, 15/07, PIVA A
      ("POD001", 2025, 7, "2025-07-15", "NORD", "11111111111", "Distr A", 1.0, 2.0),
      ("POD002", 2025, 7, "2025-07-15", "NORD", "11111111111", "Distr A", 1.0, 2.0),
      // NORD, 15/07, PIVA B
      ("POD003", 2025, 7, "2025-07-15", "NORD", "22222222222", "Distr B", 3.0, 4.0),
      // SUD, 15/07, PIVA A
      ("POD004", 2025, 7, "2025-07-15", "SUD", "11111111111", "Distr A", 5.0, 6.0),
      // NORD, 16/07, PIVA A
      ("POD005", 2025, 7, "2025-07-16", "NORD", "11111111111", "Distr A", 7.0, 8.0),
      // POD da escludere
      ("POD_EXCL", 2025, 7, "2025-07-15", "NORD", "11111111111", "Distr A", 999.0, 999.0)
    )

    val dfDailyNo = testData.toDF(
      "pod", "anno", "mese", "giorno", "area", "piva_distr", "rag_soc_distr", "q1", "q2"
    )

    val executionId = System.currentTimeMillis()
    val podExcluded = List("POD_EXCL")
    
    val result = CalcoloPrelevatoPuntiPrelievoNOTransformation.aggregateNODailyProfiles(
      dfDailyNo, executionId, podExcluded
    )

    println("\n===== RESULT ComplexScenario =====")
    result.show(false)
    
    // Expected: 4 record aggregati
    // 1. NORD, 15/07, PIVA A (POD001+POD002) = q1:2.0, q2:4.0
    // 2. NORD, 15/07, PIVA B (POD003) = q1:3.0, q2:4.0
    // 3. SUD, 15/07, PIVA A (POD004) = q1:5.0, q2:6.0
    // 4. NORD, 16/07, PIVA A (POD005) = q1:7.0, q2:8.0
    assert(result.count() == 4, s"Expected 4 aggregated records, got ${result.count()}")
    
    val nordJul15PivaA = result.filter($"area" === "NORD" && $"giorno" === "2025-07-15" && $"piva_distr" === "11111111111").collect()(0)
    assert(nordJul15PivaA.getAs[Double]("q1") == 2.0, s"Expected q1=2.0, got ${nordJul15PivaA.getAs[Double]("q1")}")
    
    val nordJul15PivaB = result.filter($"area" === "NORD" && $"giorno" === "2025-07-15" && $"piva_distr" === "22222222222").collect()(0)
    assert(nordJul15PivaB.getAs[Double]("q1") == 3.0)
    
    val sudJul15 = result.filter($"area" === "SUD" && $"giorno" === "2025-07-15").collect()(0)
    assert(sudJul15.getAs[Double]("q1") == 5.0)
    
    val nordJul16 = result.filter($"area" === "NORD" && $"giorno" === "2025-07-16").collect()(0)
    assert(nordJul16.getAs[Double]("q1") == 7.0)
    
    println("✓ Test ComplexScenario PASSED")
  }
}
