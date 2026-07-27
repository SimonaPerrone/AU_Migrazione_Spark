package it.eng.au.portale_consumi_ee.trasformations

import it.eng.au.portale_consumi_ee.Main.logger
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure._
import it.eng.au.portale_consumi_ee.schema.misure._
import it.eng.au.portale_consumi_ee.utility.functions.argumentsUtilitiesExport
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object registroLoadTrasformation {
  val spark: SparkSession = EnvironmentMisure.getSpark
  import spark.implicits._


  //fase di stage
  def controlloEtlPrecedente(etlStage3m2: Dataset[etlStage3M2PreRunModel], registroLoad: Dataset[registroLoadModel]
                            ): Boolean = {

    val totCodPodStage = etlStage3m2.select(col(etlStage3M2ProposedSchema.competenza_consumi)).distinct().count()
    val totCodPodLoad = registroLoad.select(col(registroLoadSchema.competenza_consumi)).distinct().count()

    var dfCodPodStage: Option[Int] = None
    var dfCodPodLoad: Option[Int] = None
    var isEtlPrecedenteOk=true

    // ottengo la partizione "più grande" di stage
    if (totCodPodStage > 0) {
      dfCodPodStage = Some(etlStage3m2.select(col(etlStage3M2ProposedSchema.cod_pod)).distinct().orderBy(desc(etlStage3M2ProposedSchema.cod_pod))
        .collect()(0).getString(0).toInt)
    }
    //ottengo la partizione più grande di registro
    if (totCodPodLoad >0) {
      dfCodPodLoad = Some(registroLoad.select(col(etlStage3M2ProposedSchema.cod_pod)).distinct().orderBy(desc(registroLoadSchema.competenza_consumi))
        .collect()(0).getString(0).toInt)
    }

    if(dfCodPodStage==dfCodPodLoad && totCodPodStage==totCodPodLoad){
      isEtlPrecedenteOk=true
      logger.info(s"ETL-PRECEDENTE-OK")
    }else{
      isEtlPrecedenteOk=false
      logger.info(s"ETL-PRECEDENTE-KO")

    }

    isEtlPrecedenteOk
  }
  //fase di export
  def controlloEtlCorrente(etlStage3m2: Dataset[etlStage3M2PreRunModel], registroLoad: Dataset[registroLoadModel]
                            ): Boolean = {

    val totCodPodStage = etlStage3m2.select(col(etlStage3M2ProposedSchema.competenza_consumi)).distinct().count()
    val totCodPodLoad = registroLoad.select(col(registroLoadSchema.competenza_consumi)).distinct().count()

    var dfCodPodStage: Option[Int] = None
    var dfCodPodLoad: Option[Int] = None
    var isEtlPrecedenteOk=true

    // ottengo la partizione "più grande" di stage
    if (totCodPodStage > 0) {
      dfCodPodStage = Some(etlStage3m2.select(col(etlStage3M2ProposedSchema.cod_pod)).distinct().orderBy(desc(etlStage3M2ProposedSchema.cod_pod))
        .collect()(0).getString(0).toInt)
    }
    //ottengo la partizione più grande di registro
    if (totCodPodLoad >0) {
      dfCodPodLoad = Some(registroLoad.select(col(etlStage3M2ProposedSchema.cod_pod)).distinct().orderBy(desc(registroLoadSchema.competenza_consumi))
        .collect()(0).getString(0).toInt)
    }

    if(dfCodPodStage==dfCodPodLoad && totCodPodStage==totCodPodLoad){
      isEtlPrecedenteOk=true
      logger.info(s"ETL-PRECEDENTE-OK")
    }else{
      isEtlPrecedenteOk=false
      logger.info(s"ETL-PRECEDENTE-KO")

    }

    isEtlPrecedenteOk
  }

  def updateRegistroLoad(etlStage: Dataset[etlStage3M2ProposedModel], storic:Boolean
                            ): Dataset[registroLoadModel] = {


    val id_run = if (storic) "33M" else "3M"
    val last_run = argumentsUtilitiesExport.convertUtcToLong("UTC")
    val note = "ETL_forzato"

    val finalDfRegistroLoad = etlStage.groupBy(etlStage3M2ProposedSchema.competenza_consumi)
      .agg(count("*").cast(LongType).alias(registroLoadSchema.numero_documenti))
      .withColumn(registroLoadSchema.id_run,lit(id_run))
      .withColumn(registroLoadSchema.last_run,lit(last_run))
      .withColumn(registroLoadSchema.note,lit(note))

      val output = finalDfRegistroLoad
      .selectExpr(registroLoadSchema.getValues:_*)
      .as[registroLoadModel]

    output
  }

  def deleteDataRegistroLoad(storic:Boolean
                        ): (String, Int) = {

    val id_run = if (storic) "33M" else "3M"
    val windowTimeValue = EnvironmentMisure.getProperty("spark.app.mongodb.delay")
    val timeZone = EnvironmentMisure.getProperty("spark.app.time_zone")
    val monthLimitFlow3M = argumentsUtilitiesExport.annomeseDefiniton(windowTimeValue,timeZone)
    val month34 = argumentsUtilitiesExport.get37thMonthAgo()
    val annomese = if (storic) month34 else monthLimitFlow3M

    (id_run,annomese)
  }

}
