package it.eng.au.portale_consumi_ee.flow.Prova


import it.eng.au.portale_consumi_ee.Main.logger
import it.eng.au.portale_consumi_ee.common.flow.{FlowDsOutput, FlowUnitOutput}
import it.eng.au.portale_consumi_ee.dao.mongo.ProvaDao
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.Forniture.MisureElettricheModel
import it.eng.au.portale_consumi_ee.model.misure.{AutoletturaValues, VoltureValues, etlStage3M2ProposedModel, misureMensiliCStructValues, misureNonOrarieCStructValues, misureOrarieCStructValues}
import it.eng.au.portale_consumi_ee.schema.Forniture.MisureElettricheSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

class Prova(implicit spark: SparkSession)  extends FlowUnitOutput{

  override def run() = {
    import spark.implicits._

    logger.info("prova")
    logger.info("Starting FornitureInfo run")
    // Example Spark operation
    val properties = EnvironmentMisure.printProperties
    logger.info(s"Spark Environments property: $properties")

    val getProvaDao = new ProvaDao
//
//    val etlStageNewDS: Dataset[etlStage3M2ProposedModel] = Seq(
//      etlStage3M2ProposedModel("ID001", List(misureOrarieCStructValues(1, 202401, 100.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0)),
//        misureMensiliCStructValues(202401, 5.0, 100, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0),
//        misureNonOrarieCStructValues(202401, 5.0, 95.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0),
//        VoltureValues(202401, 1706784000L, 90.0, 95.0, 85.0, 75.0, 65.0, 55.0, "FLUSSO1"),
//        AutoletturaValues(202401, 1706784000L, 88.0, 78.0, 68.0, 58.0, 48.0, 38.0), "POD001", "COD001", "hashed1_updated",20250210,202401
//      )
//    ).toDS()
//
//    val prova = etlStageNewDS
//      .withColumn(MisureElettricheSchema.codice_fornitura,lit("ID001"))
//      .withColumn(MisureElettricheSchema.pod,lit("POD001"))
//      .withColumn(MisureElettricheSchema._id,concat(col(MisureElettricheSchema.codice_fornitura),col(MisureElettricheSchema.pod),lit("_"),lit(202401)))
//      .selectExpr(MisureElettricheSchema.getValues : _*)
//      .as[MisureElettricheModel]
//
//    val schemaProva = prova.printSchema()
//    val showProva = prova.show()
//    logger.info("print  prova")
//    println("print  prova")
//    logger.info(s"print  schema: ${schemaProva}")
//    println(s"print  schema: ${schemaProva}")
//    logger.info(s"print  show: ${showProva}")
//    println(s"print  show: ${showProva}")
//
//
//    val etlStageNewDS1: Dataset[etlStage3M2ProposedModel] = Seq(
//      etlStage3M2ProposedModel("ID001", List(misureOrarieCStructValues(1, 202401, 100.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0)),
//        misureMensiliCStructValues(202401, 5.0, 100, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0),
//        misureNonOrarieCStructValues(202401, 5.0, 95.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0),
//        VoltureValues(202401, 1706784000L, 90.0, 95.0, 85.0, 75.0, 65.0, 55.0, "FLUSSO1"),
//        AutoletturaValues(202401, 1706784000L, 88.0, 78.0, 68.0, 58.0, 48.0, 38.0), "POD001", "COD001", "hashed1_updated",20250210,202402
//      )
//    ).toDS()
//
//    val prova1 = etlStageNewDS1
//      .withColumn(MisureElettricheSchema.codice_fornitura,lit("ID001"))
//      .withColumn(MisureElettricheSchema.pod,lit("POD001"))
//      .withColumn(MisureElettricheSchema._id,concat(col(MisureElettricheSchema.codice_fornitura),col(MisureElettricheSchema.pod),lit("_"),lit(202402)))
//      .selectExpr(MisureElettricheSchema.getValues : _*)
//      .as[MisureElettricheModel]
//
//    val schemaProva1 = prova1.printSchema()
//    val showProva1 = prova1.show()
//    logger.info("print  prova1")
//    println("print  prova1")
//    logger.info(s"print  schema: ${schemaProva1}")
//    println(s"print  schema: ${schemaProva1}")
//    logger.info(s"print  show: ${showProva1}")
//    println(s"print  show: ${showProva1}")
//
//
//    //forth value
//
//    val etlStageNewDS4: Dataset[etlStage3M2ProposedModel] = Seq(
//      etlStage3M2ProposedModel("ID007", List(misureOrarieCStructValues(1, 202401, 100.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0)),
//        misureMensiliCStructValues(202401, 5.0, 100, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0),
//        misureNonOrarieCStructValues(202401, 5.0, 95.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0),
//        VoltureValues(202401, 1706784000L, 90.0, 95.0, 85.0, 75.0, 65.0, 55.0, "FLUSSO7"),
//        AutoletturaValues(202401, 1706784000L, 88.0, 78.0, 68.0, 58.0, 48.0, 38.0), "POD007", "COD007", "hashed7_updated",20100101,201001
//      )
//    ).toDS()
//
//    val prova4 = etlStageNewDS4
//      .withColumn(MisureElettricheSchema.codice_fornitura,lit("ID007"))
//      .withColumn(MisureElettricheSchema.pod,lit("POD007"))
//      .withColumn(MisureElettricheSchema._id,concat(col(MisureElettricheSchema.codice_fornitura),col(MisureElettricheSchema.pod),lit("_"),lit(202402)))
//      .selectExpr(MisureElettricheSchema.getValues : _*)
//      .as[MisureElettricheModel]
//
//    val schemaProva4 = prova4.printSchema()
//    val showProva4 = prova4.show()
//    logger.info("print  prova4")
//    println("print  prova4")
//    logger.info(s"print  schema: ${schemaProva4}")
//    println(s"print  schema: ${schemaProva4}")
//    logger.info(s"print  show: ${showProva4}")
//    println(s"print  show: ${showProva4}")
//
//    //third value
//
//    val etlStageNewDS2: Dataset[etlStage3M2ProposedModel] = Seq(
//      etlStage3M2ProposedModel("ID002", List(misureOrarieCStructValues(1, 202401, 100.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0)),
//        misureMensiliCStructValues(202401, 5.0, 100, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0),
//        misureNonOrarieCStructValues(202401, 5.0, 95.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0),
//        VoltureValues(202401, 1706784000L, 90.0, 95.0, 85.0, 75.0, 65.0, 55.0, "FLUSSO1"),
//        AutoletturaValues(202401, 1706784000L, 88.0, 78.0, 68.0, 58.0, 48.0, 38.0), "POD002", "COD001", "hashed1_updated",20250211,202401
//      )
//    ).toDS()
//
//    val prova2 = etlStageNewDS2
//      .withColumn(MisureElettricheSchema.codice_fornitura,lit("ID002"))
//      .withColumn(MisureElettricheSchema.pod,lit("POD002"))
//      .withColumn(MisureElettricheSchema._id,concat(col(MisureElettricheSchema.codice_fornitura),col(MisureElettricheSchema.pod),lit("_"),lit(202402)))
//      .selectExpr(MisureElettricheSchema.getValues : _*)
//      .as[MisureElettricheModel]
//
//
//    val schemaProva2 = prova2.printSchema()
//    val showProva2 = prova2.show()
//    logger.info("print  prova2")
//    println("print  prova2")
//    logger.info(s"print  schema: ${schemaProva2}")
//    println(s"print  schema: ${schemaProva2}")
//    logger.info(s"print  show: ${showProva2}")
//    println(s"print  show: ${showProva2}")
//
//
//    val provaFinal = prova.unionByName(prova2).unionByName(prova1).unionByName(prova4)
//    logger.info("print inizio scrittura su mongo")
//    getProvaDao.write(provaFinal)
//    logger.info("print fine scrittura su mongo")

    //todo next step
    //delete annomese 202402
    getProvaDao.removeOldPartition(202402)

    val etlStageNewDS3: Dataset[etlStage3M2ProposedModel] = Seq(
      etlStage3M2ProposedModel(
        "ID001",
        List(misureOrarieCStructValues( giorno = "20240101","202401", "100.0", "10.0", "20.0", "30.0", "40.0", "50.0", "60.0",data_lettura = "20240101")),
        misureMensiliCStructValues("202401", "5.0", "100", "10.0", "20.0", "30.0", "40.0", "50.0", "60.0"),
        misureNonOrarieCStructValues("202401", "5.0", "95.0", "10.0", "20.0", "30.0", "40.0", "50.0", "60.0"),
        VoltureValues("202401", "1706784000", "90.0", "95.0", "85.0", "75.0", "65.0", "55.0", "FLUSSO1_new"),
        AutoletturaValues("202401", "1706784000", "88.0", "78.0", "68.0", "58.0", "48.0", "38.0"),
        "POD001", "COD001_new", "hashed1_newone", 202401, 202401
      )
    ).toDS()

    val prova3 = etlStageNewDS3
      .withColumn(MisureElettricheSchema.codice_fornitura,lit("ID001"))
      .withColumn(MisureElettricheSchema.pod,lit("POD001"))
      .withColumn(MisureElettricheSchema._id,concat(col(MisureElettricheSchema.codice_fornitura),col(MisureElettricheSchema.pod),lit("_"),lit(202401)))
      .selectExpr(MisureElettricheSchema.getValues : _*)
      .as[MisureElettricheModel]


//    val schemaProva3 = prova3.printSchema()
//    val showProva3 = prova3.show()
//    logger.info("print  prova3")
//    println("print  prova3")
//    logger.info(s"print  schema: ${schemaProva3}")
//    println(s"print  schema: ${schemaProva3}")
//    logger.info(s"print  show: ${showProva3}")
//    println(s"print  show: ${showProva3}")
//
//    getProvaDao.saveWithUpdate(prova3)

  }

}
