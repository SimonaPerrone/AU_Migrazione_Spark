package it.eng.au.portale_consumi_ee.trasformations

import it.eng.au.portale_consumi_ee.Main.logger
import it.eng.au.portale_consumi_ee.common.utility.functions.costants.DOT
import it.eng.au.portale_consumi_ee.dao.hive.CheckPointUtils
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure._
import it.eng.au.portale_consumi_ee.schema.misure._
import it.eng.au.portale_consumi_ee.utility.functions.argumentsUtilitiesExport
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object stagePhaseTrasformationDifferentAprroach {
  val spark: SparkSession = EnvironmentMisure.getSpark

  import spark.implicits._
  val autolettura_hashed = "autolettura_hashed"
  val autolettura = "autolettura"
  val volture = "volture"
  val volture_hashed= "volture_hashed"
  val misura_oraria_mese = "misura_oraria_mese"
  val misura_oraria_mese_hashed = "misura_oraria_mese_hashed"
  val misura_non_oraria = "misura_non_oraria"
  val misura_non_oraria_hashed = "misura_non_oraria_hashed"
  val misura_oraria_gg = "misura_oraria_gg"
  val misura_oraria_gg_hashed = "misura_oraria_gg_hashed"
  val giorno = "giorno"
  val maxG = "maxG"
  val misura_oraria_gg_element = "misura_oraria_gg_element"

  //private attribure
  private val n_id_fornitura_1 = "n_id_fornitura_1"
  private val n_id_fornitura_2 = "n_id_fornitura_2"
  private val pod_1 ="pod_1"
  private val pod_2 ="pod_2"
  private val competenza_consumi_1 = "competenza_consumi_1"
  private val competenza_consumi_2 = "competenza_consumi_2"
  private val first_hash = "first_hash"
  private val second_hash = "second_hash"
  private val third_hash= "third_hash"
  private val stage1 = "stage1"
  private val stage2 = "stage2"
  private val stage3 = "stage3"
  private val stage4 = "stage4"


  def autolettureGeneration(autolettureDS: Dataset[autolettureModel]): Dataset[autoletturaRevisitedModel] = {

    def autolettureDSHashed = autolettureDS
      .withColumn(
        autolettura,
        concat(
          coalesce(col(autolettureSchema.n_id_fornitura), lit("")),
          coalesce(col(autolettureSchema.pod), lit("")),
          coalesce(col(autolettureSchema.competenza_consumi), lit("")),
          coalesce(col(autolettureSchema.data_lettura), lit("")),
          coalesce(col(autolettureSchema.lettura_misura_monoraria), lit("")),
          coalesce(col(autolettureSchema.lettura_misura_f1), lit("")),
          coalesce(col(autolettureSchema.lettura_misura_f2), lit("")),
          coalesce(col(autolettureSchema.lettura_misura_f3), lit("")),
          coalesce(col(autolettureSchema.lettura_misura_f4), lit("")),
          coalesce(col(autolettureSchema.lettura_misura_f5), lit("")),
          coalesce(col(autolettureSchema.lettura_misura_f6), lit(""))
        )
      )
      .withColumn(autolettura_hashed, sha2(col(autolettura),256))
      .drop(autolettura)
      .as[autolettureHashed]

    autolettureDSHashed
      .map(r =>  autoletturaRevisitedModel(
      n_id_fornitura = Option(r.n_id_fornitura).map(_.toString).getOrElse(""),
      pod = Option(r.pod).map(_.toString).getOrElse(""),
      competenza_consumi = r.competenza_consumi,
      autolettura = AutoletturaValues(
              competenza_consumi = Option(r.competenza_consumi).map(_.toString).getOrElse(""),
              data_lettura = Option(r.data_lettura).map(_.toString).getOrElse(""),
              lettura_misura_monoraria = Option(r.lettura_misura_monoraria).map(_.toString).getOrElse(""),
              lettura_misura_f1 = Option(r.lettura_misura_f1).map(_.toString).getOrElse(""),
              lettura_misura_f2 = Option(r.lettura_misura_f2).map(_.toString).getOrElse(""),
              lettura_misura_f3 = Option(r.lettura_misura_f3).map(_.toString).getOrElse(""),
              lettura_misura_f4 = Option(r.lettura_misura_f4).map(_.toString).getOrElse(""),
              lettura_misura_f5 = Option(r.lettura_misura_f5).map(_.toString).getOrElse(""),
              lettura_misura_f6 = Option(r.lettura_misura_f6).map(_.toString).getOrElse("")
      ),
        autolettura_hashed = r.autolettura_hashed
    ))
  }

  def voltureGeneration(voltureDS: Dataset[voltureModel]): Dataset[voltureRevisitedModel] = {

    def voltureDSHashed = voltureDS
      .withColumn(
        volture,
        concat(
          coalesce(col(voltureSchema.competenza_consumi), lit("")),
          coalesce(col(voltureSchema.data_lettura), lit("")),
          coalesce(col(voltureSchema.lettura_misura_monoraria), lit("")),
          coalesce(col(voltureSchema.lettura_misura_f1), lit("")),
          coalesce(col(voltureSchema.lettura_misura_f2), lit("")),
          coalesce(col(voltureSchema.lettura_misura_f3), lit("")),
          coalesce(col(voltureSchema.lettura_misura_f4), lit("")),
          coalesce(col(voltureSchema.lettura_misura_f5), lit("")),
          coalesce(col(voltureSchema.lettura_misura_f6), lit("")),
          coalesce(col(voltureSchema.tipo_flusso2), lit(""))
        )
      )
      .withColumn(volture_hashed, sha2(col(volture),256))
      .drop(volture)
      .as[voltureHashedModel]

    voltureDSHashed
      .map(r =>  voltureRevisitedModel(
        n_id_fornitura = Option(r.n_id_fornitura).map(_.toString).getOrElse(""),
        pod = Option(r.pod).map(_.toString).getOrElse(""),
        competenza_consumi = r.competenza_consumi,
        volture = VoltureValues(
          competenza_consumi =Option(r.competenza_consumi).map(_.toString).getOrElse(""),
          data_lettura = Option(r.data_lettura).map(_.toString).getOrElse(""),
          lettura_misura_monoraria = Option(r.lettura_misura_monoraria).map(_.toString).getOrElse(""),
          lettura_misura_f1 = Option(r.lettura_misura_f1).map(_.toString).getOrElse(""),
          lettura_misura_f3 = Option(r.lettura_misura_f3).map(_.toString).getOrElse(""),
          lettura_misura_f4 = Option(r.lettura_misura_f4).map(_.toString).getOrElse(""),
          lettura_misura_f5 = Option(r.lettura_misura_f5).map(_.toString).getOrElse(""),
          lettura_misura_f6 = Option(r.lettura_misura_f6).map(_.toString).getOrElse(""),
          tipo_misura = argumentsUtilitiesExport.getDescrTipoMisura(Option(r.tipo_flusso2).map(_.toString).getOrElse(""))
        ),
        volture_hashed = r.volture_hashed
      ))

  }

  def misureMensiliGeneration(misureMensiliCDS: Dataset[misureMensiliCModel]): Dataset[misureMensiliCRevisitedModel] = {

    // Define a window specification for calculating max(giorno)
    val windowMaxGiorno = Window.partitionBy(misureMensiliCSchema.n_id_fornitura, misureMensiliCSchema.pod, misureMensiliCSchema.competenza_consumi)

    def misureMensiliCDSHashed = misureMensiliCDS
      .withColumn(
        misura_oraria_mese,
        concat(
          coalesce(col(misureMensiliCSchema.competenza_consumi), lit("")),
          coalesce(col(misureMensiliCSchema.delta_misura_monoraria), lit("")),
          coalesce(col(misureMensiliCSchema.lettura_misura_monoraria), lit("")),
          coalesce(col(misureMensiliCSchema.lettura_misura_f1), lit("")),
          coalesce(col(misureMensiliCSchema.lettura_misura_f2), lit("")),
          coalesce(col(misureMensiliCSchema.lettura_misura_f3), lit("")),
          coalesce(col(misureMensiliCSchema.lettura_misura_f4), lit("")),
          coalesce(col(misureMensiliCSchema.lettura_misura_f5), lit("")),
          coalesce(col(misureMensiliCSchema.lettura_misura_f6), lit("")),
          coalesce(col(misureMensiliCSchema.delta_misure_f1), lit("")),
          coalesce(col(misureMensiliCSchema.delta_misure_f2), lit("")),
          coalesce(col(misureMensiliCSchema.delta_misure_f3), lit("")),
          coalesce(col(misureMensiliCSchema.delta_misure_f4), lit("")),
          coalesce(col(misureMensiliCSchema.delta_misure_f5), lit("")),
          coalesce(col(misureMensiliCSchema.delta_misure_f6), lit("")),
          coalesce(col(misureMensiliCSchema.tipo_flusso), lit("")),
          coalesce(col(misureMensiliCSchema.data_lettura), lit(""))
        )
      )
      .withColumn(misura_oraria_mese_hashed, sha2(col(misura_oraria_mese),256))
      .withColumn(giorno, substring(col(misureMensiliCSchema.data_lettura), 7, 2).cast("int"))
      .withColumn(
        maxG,
        max(substring(col(misureMensiliCSchema.data_lettura), 7, 2).cast("int")).over(windowMaxGiorno)
      )
      .filter(col(giorno) === col(maxG))
      .drop(misura_oraria_mese)
      .drop(maxG)
      .drop(giorno)
      .as[misureMensiliCModelHashed]

    misureMensiliCDSHashed
      .map(r =>  misureMensiliCRevisitedModel(
        n_id_fornitura = r.n_id_fornitura,
        pod = r.pod,
        competenza_consumi = r.competenza_consumi,
        misura_oraria_mese = misureMensiliCStructValues(
          competenza_consumi = Option(r.competenza_consumi).map(_.toString).getOrElse(""),
            delta_misure_monoraria = Option(r.delta_misura_monoraria).map(_.toString).getOrElse(""),
            lettura_misura_monoraria = Option(r.lettura_misura_monoraria).map(_.toString).getOrElse(""),
            lettura_misura_f1 = Option(r.lettura_misura_f1).map(_.toString).getOrElse(""),
            lettura_misura_f2 = Option(r.lettura_misura_f2).map(_.toString).getOrElse(""),
            lettura_misura_f3 = Option(r.lettura_misura_f3).map(_.toString).getOrElse(""),
            lettura_misura_f4 = Option(r.lettura_misura_f4).map(_.toString).getOrElse(""),
            lettura_misura_f5 = Option(r.lettura_misura_f5).map(_.toString).getOrElse(""),
            lettura_misura_f6 = Option(r.lettura_misura_f6).map(_.toString).getOrElse(""),
            delta_misure_f1 = Option(r.delta_misure_f1).map(_.toString).getOrElse(""),
            delta_misure_f2 = Option(r.delta_misure_f2).map(_.toString).getOrElse(""),
            delta_misure_f3 = Option(r.delta_misure_f3).map(_.toString).getOrElse(""),
            delta_misure_f4 = Option(r.delta_misure_f4).map(_.toString).getOrElse(""),
            delta_misure_f5 = Option(r.delta_misure_f5).map(_.toString).getOrElse(""),
            delta_misure_f6 = Option(r.delta_misure_f6).map(_.toString).getOrElse(""),
            tipo_misura = argumentsUtilitiesExport.getDescrTipoMisura(Option(r.tipo_flusso).map(_.toString).getOrElse("")),
            data_lettura = Option(r.data_lettura).map(_.toString).getOrElse(""),
            potf1 = null,
            potf2 = null,
            potf3 = null,
            potm = null
        ),
        misura_oraria_mese_hashed = r.misura_oraria_mese_hashed
      ))
  }

  def misureNonOrarieGeneration(misureNonOrarieDS: Dataset[misureNonOrarieCModel]): Dataset[misureNonOrarieCRevisitedModel] = {

    def misureMensiliCDSHashed = misureNonOrarieDS
      .withColumn(
        misura_non_oraria,
        concat(
          coalesce(col(misureNonOrarieCSchema.competenza_consumi), lit("")),
          coalesce(col(misureNonOrarieCSchema.delta_misura_monoraria), lit("")),
          coalesce(col(misureNonOrarieCSchema.lettura_misura_monoraria), lit("")),
          coalesce(col(misureNonOrarieCSchema.lettura_misura_f1), lit("")),
          coalesce(col(misureNonOrarieCSchema.lettura_misura_f2), lit("")),
          coalesce(col(misureNonOrarieCSchema.lettura_misura_f3), lit("")),
          coalesce(col(misureNonOrarieCSchema.lettura_misura_f4), lit("")),
          coalesce(col(misureNonOrarieCSchema.lettura_misura_f5), lit("")),
          coalesce(col(misureNonOrarieCSchema.lettura_misura_f6), lit("")),
          coalesce(col(misureNonOrarieCSchema.delta_misure_f1), lit("")),
          coalesce(col(misureNonOrarieCSchema.delta_misure_f2), lit("")),
          coalesce(col(misureNonOrarieCSchema.delta_misure_f3), lit("")),
          coalesce(col(misureNonOrarieCSchema.delta_misure_f4), lit("")),
          coalesce(col(misureNonOrarieCSchema.delta_misure_f5), lit("")),
          coalesce(col(misureNonOrarieCSchema.delta_misure_f6), lit("")),
          coalesce(col(misureNonOrarieCSchema.tipo_flusso2), lit(" ")),
          coalesce(col(misureNonOrarieCSchema.data_lettura), lit("")),
          coalesce(col(misureNonOrarieCSchema.potf1), lit("")),
          coalesce(col(misureNonOrarieCSchema.potf2), lit("")),
          coalesce(col(misureNonOrarieCSchema.potf3), lit("")),
          coalesce(col(misureNonOrarieCSchema.potm), lit(""))
        )
      )
      .withColumn(misura_non_oraria_hashed, sha2(col(misura_non_oraria),256))
      .drop(misura_non_oraria)
      .as[misureNonOrarieCModelHashed]

    misureMensiliCDSHashed
      .map(r =>  misureNonOrarieCRevisitedModel(
        n_id_fornitura = r.n_id_fornitura,
        pod = r.pod,
        competenza_consumi = r.competenza_consumi,
        misura_non_oraria = misureNonOrarieCStructValues(
          competenza_consumi = Option(r.competenza_consumi).map(_.toString).getOrElse(""),
          delta_misure_monoraria = Option(r.delta_misura_monoraria).map(_.toString).getOrElse(""),
          lettura_misura_monoraria = Option(r.lettura_misura_monoraria).map(_.toString).getOrElse(""),
          lettura_misura_f1 = Option(r.lettura_misura_f1).map(_.toString).getOrElse(""),
          lettura_misura_f2 = Option(r.lettura_misura_f2).map(_.toString).getOrElse(""),
          lettura_misura_f3 = Option(r.lettura_misura_f3).map(_.toString).getOrElse(""),
          lettura_misura_f4 = Option(r.lettura_misura_f4).map(_.toString).getOrElse(""),
          lettura_misura_f5 = Option(r.lettura_misura_f5).map(_.toString).getOrElse(""),
          lettura_misura_f6 = Option(r.lettura_misura_f6).map(_.toString).getOrElse(""),
          delta_misure_f1 = Option(r.delta_misure_f1).map(_.toString).getOrElse(""),
          delta_misure_f2 = Option(r.delta_misure_f2).map(_.toString).getOrElse(""),
          delta_misure_f3 = Option(r.delta_misure_f3).map(_.toString).getOrElse(""),
          delta_misure_f4 = Option(r.delta_misure_f4).map(_.toString).getOrElse(""),
          delta_misure_f5 = Option(r.delta_misure_f5).map(_.toString).getOrElse(""),
          delta_misure_f6 = Option(r.delta_misure_f6).map(_.toString).getOrElse(""),
          tipo_misura = argumentsUtilitiesExport.getDescrTipoMisura(Option(r.tipo_flusso2).map(_.toString).getOrElse("")),
          data_lettura = Option(r.data_lettura).map(_.toString).getOrElse(""),
          potf1 = Option(r.potf1).map(_.toString).getOrElse(""),
          potf2 = Option(r.potf2).map(_.toString).getOrElse(""),
          potf3 = Option(r.potf3).map(_.toString).getOrElse(""),
          potm = Option(r.potm).map(_.toString).getOrElse("")
        ),
        misura_non_oraria_hashed = r.misura_non_oraria_hashed
      ))

  }

  def misureOrarieGeneration(misureOrarieDS: Dataset[misureOrarieCModel]): Dataset[misureOrarieCRevisitedModel] = {

    //first step define misure orarie structure
    val misuraGiornalieraElement = misureOrarieDS
      .map(r =>  misureOrarieCRevisitedModelSingleElement(
        giorno =r.giorno,
        n_id_fornitura = r.n_id_fornitura,
        pod = r.pod,
        competenza_consumi = r.competenza_consumi,
        misura_oraria_gg_element = misureOrarieCStructValues(
          giorno = Option(r.data_lettura).map(_.toString).getOrElse(""),
          competenza_consumi = Option(r.competenza_consumi).map(_.toString).getOrElse(""),
          consumo_giornaliero_gg = Option(r.consumo_giornaliero_gg).map(_.toString).getOrElse(""),
          lettura_misura_f1 = Option(r.lettura_giornaliero_f1).map(_.toString).getOrElse(""),
          lettura_misura_f2 = Option(r.lettura_giornaliero_f2).map(_.toString).getOrElse(""),
          lettura_misura_f3 = Option(r.lettura_giornaliero_f3).map(_.toString).getOrElse(""),
          lettura_misura_f4 = Option(r.lettura_giornaliero_f4).map(_.toString).getOrElse(""),
          lettura_misura_f5 = Option(r.lettura_giornaliero_f5).map(_.toString).getOrElse(""),
          lettura_misura_f6 = Option(r.lettura_giornaliero_f6).map(_.toString).getOrElse(""),
          delta_misure_f1 = Option(r.delta_misure_f1).map(_.toString).getOrElse(""),
          delta_misure_f2 = Option(r.delta_misure_f2).map(_.toString).getOrElse(""),
          delta_misure_f3 = Option(r.delta_misure_f3).map(_.toString).getOrElse(""),
          delta_misure_f4 = Option(r.delta_misure_f4).map(_.toString).getOrElse(""),
          delta_misure_f5 = Option(r.delta_misure_f5).map(_.toString).getOrElse(""),
          delta_misure_f6 = Option(r.delta_misure_f6).map(_.toString).getOrElse(""),
          potenza_max_erogata = Option(r.potenza_max_erogata).map(_.toString).getOrElse(""),
          tipo_misura = argumentsUtilitiesExport.getDescrTipoMisura(Option(r.tipo_flusso).map(_.toString).getOrElse("")),
          data_lettura = Option(r.data_lettura).map(_.toString).getOrElse("")
        )
      )).as[misureOrarieCRevisitedModelSingleElement]

    //step group for competenze_consumi
    val misuraGiornalieraMonthElementsGroup = misuraGiornalieraElement
      .groupBy(misureOrarieCSchema.n_id_fornitura,misureOrarieCSchema.pod,misureOrarieCSchema.competenza_consumi)
      .agg(
        sort_array(collect_list(misura_oraria_gg_element)).as(misura_oraria_gg)
      )
      .withColumn(misura_oraria_gg_hashed,sha2(to_json(col(misura_oraria_gg)),256))

    misuraGiornalieraMonthElementsGroup
      .as[misureOrarieCRevisitedModel]
  }


  // calcolo stage data on this run
  def calcolo_stage(
                     autolettureDS: Dataset[autoletturaRevisitedModel],
                     misureMensiliCDS: Dataset[misureMensiliCRevisitedModel],
                     misureNonOrarieCDS: Dataset[misureNonOrarieCRevisitedModel],
                     misureOrarieCDS: Dataset[misureOrarieCRevisitedModel],
                     voltureDS: Dataset[voltureRevisitedModel]
                   ): Dataset[etlStage3M2ProposedModel] = {

    val timeZone = EnvironmentMisure.getProperty("spark.app.time_zone")
    val currentAnnoMeseGiornoLong = argumentsUtilitiesExport.convertUtcToLong(timeZone)

//    val firstJoin = misureOrarieCDS.join(misureMensiliCDS,
//        misureOrarieCDS(misureOrarieCSchema.n_id_fornitura) === misureMensiliCDS(misureMensiliCSchema.n_id_fornitura) &&
//        misureOrarieCDS(misureOrarieCSchema.pod) === misureMensiliCDS(misureMensiliCSchema.pod) &&
//        misureOrarieCDS(misureOrarieCSchema.competenza_consumi) === misureMensiliCDS(misureMensiliCSchema.competenza_consumi)
//      ,"full_outer")
//      .select(
//        misureOrarieCDS(misureOrarieCSchema.n_id_fornitura).as(n_id_fornitura_1),
//        misureOrarieCDS(misureOrarieCSchema.pod).as(pod_1),
//        misureOrarieCDS(misureOrarieCSchema.competenza_consumi).as(competenza_consumi_1),
//        misureOrarieCDS(misura_oraria_gg).as(etlStage3M2ProposedSchema.misure_orarie),
//        misureOrarieCDS(misura_oraria_gg_hashed).as(misura_oraria_gg_hashed),
//        misureMensiliCDS(misureMensiliCSchema.n_id_fornitura).as(n_id_fornitura_2),
//        misureMensiliCDS(misureMensiliCSchema.pod).as(pod_2),
//        misureMensiliCDS(misureMensiliCSchema.competenza_consumi).as(competenza_consumi_2),
//        misureMensiliCDS(misura_oraria_mese).as(etlStage3M2ProposedSchema.misure_mensili),
//        misureMensiliCDS(misura_oraria_mese_hashed).as(misura_oraria_mese_hashed)
//      )
//      .withColumn(first_hash,sha2(concat_ws("_",col(misura_oraria_gg_hashed),col(misura_oraria_mese_hashed)),256))
//      .withColumn(etlStage3M2ProposedSchema.n_id_fornitura,coalesce(col(n_id_fornitura_1),col(n_id_fornitura_2)))
//      .withColumn(etlStage3M2ProposedSchema.pod,coalesce(col(pod_1),col(pod_2)))
//      .withColumn(etlStage3M2ProposedSchema.competenza_consumi,coalesce(col(competenza_consumi_1),col(competenza_consumi_2)))
//      .select(
//        col(etlStage3M2ProposedSchema.n_id_fornitura),
//        col(etlStage3M2ProposedSchema.pod),
//        col(etlStage3M2ProposedSchema.competenza_consumi),
//        col(etlStage3M2ProposedSchema.misure_orarie),
//        col(etlStage3M2ProposedSchema.misure_mensili),
//        col(first_hash)
//      )

    val firstJoinCheckPoint = joinOrarieMensili(misureOrarieCDS,misureMensiliCDS)
    CheckPointUtils.writeCheckpoint(firstJoinCheckPoint, stage1)

    val firstJoin = CheckPointUtils.readCheckpoint(spark, stage1)

//    val secondJoin = firstJoin.join(misureNonOrarieCDS,
//      firstJoin(etlStage3M2ProposedSchema.n_id_fornitura) === misureNonOrarieCDS(misureNonOrarieCSchema.n_id_fornitura) &&
//        firstJoin(etlStage3M2ProposedSchema.pod) === misureNonOrarieCDS(misureNonOrarieCSchema.pod) &&
//        firstJoin(etlStage3M2ProposedSchema.competenza_consumi) === misureNonOrarieCDS(misureNonOrarieCSchema.competenza_consumi)
//      ,"full_outer")
//      .select(
//        firstJoin(misureOrarieCSchema.n_id_fornitura).as(n_id_fornitura_1),
//        firstJoin(misureOrarieCSchema.pod).as(pod_1),
//        firstJoin(misureOrarieCSchema.competenza_consumi).as(competenza_consumi_1),
//        firstJoin(etlStage3M2ProposedSchema.misure_orarie).as(etlStage3M2ProposedSchema.misure_orarie),
//        firstJoin(etlStage3M2ProposedSchema.misure_mensili).as(etlStage3M2ProposedSchema.misure_mensili),
//        firstJoin(first_hash),
//
//        misureNonOrarieCDS(misureNonOrarieCSchema.n_id_fornitura).as(n_id_fornitura_2),
//        misureNonOrarieCDS(misureNonOrarieCSchema.pod).as(pod_2),
//        misureNonOrarieCDS(misureNonOrarieCSchema.competenza_consumi).as(competenza_consumi_2),
//        misureNonOrarieCDS(misura_non_oraria).as(etlStage3M2ProposedSchema.misure_non_orarie),
//        misureNonOrarieCDS(misura_non_oraria_hashed).as(misura_non_oraria_hashed)
//      )
//      .withColumn(etlStage3M2ProposedSchema.n_id_fornitura,coalesce(col(n_id_fornitura_1),col(n_id_fornitura_2)))
//      .withColumn(etlStage3M2ProposedSchema.pod,coalesce(col(pod_1),col(pod_2)))
//      .withColumn(etlStage3M2ProposedSchema.competenza_consumi,coalesce(col(competenza_consumi_1),col(competenza_consumi_2)))
//      .withColumn(second_hash,sha2(concat_ws("_",col(first_hash),col(misura_non_oraria_hashed)),256))
//      .select(
//        col(etlStage3M2ProposedSchema.n_id_fornitura),
//        col(etlStage3M2ProposedSchema.pod),
//        col(etlStage3M2ProposedSchema.competenza_consumi),
//        col(etlStage3M2ProposedSchema.misure_orarie),
//        col(etlStage3M2ProposedSchema.misure_mensili),
//        col(etlStage3M2ProposedSchema.misure_non_orarie),
//        col(second_hash)
//      )

    val secondJoinCheckPoint = joinMisureNonOrarie(firstJoin,misureNonOrarieCDS)
    CheckPointUtils.writeCheckpoint(secondJoinCheckPoint, stage2)

    val secondJoin = CheckPointUtils.readCheckpoint(spark, stage2)

//    val thirdJoin = secondJoin.join(voltureDS,
//      secondJoin(etlStage3M2ProposedSchema.n_id_fornitura) === voltureDS(voltureSchema.n_id_fornitura) &&
//        secondJoin(etlStage3M2ProposedSchema.pod) === voltureDS(voltureSchema.pod) &&
//        secondJoin(etlStage3M2ProposedSchema.competenza_consumi) === voltureDS(voltureSchema.competenza_consumi)
//      ,"full_outer")
//      .select(
//        secondJoin(misureOrarieCSchema.n_id_fornitura).as(n_id_fornitura_1),
//        secondJoin(misureOrarieCSchema.pod).as(pod_1),
//        secondJoin(misureOrarieCSchema.competenza_consumi).as(competenza_consumi_1),
//        secondJoin(etlStage3M2ProposedSchema.misure_orarie).as(etlStage3M2ProposedSchema.misure_orarie),
//        secondJoin(etlStage3M2ProposedSchema.misure_mensili).as(etlStage3M2ProposedSchema.misure_mensili),
//        secondJoin(etlStage3M2ProposedSchema.misure_non_orarie).as(etlStage3M2ProposedSchema.misure_non_orarie),
//        secondJoin(second_hash),
//
//        voltureDS(voltureSchema.n_id_fornitura).as(n_id_fornitura_2),
//        voltureDS(voltureSchema.pod).as(pod_2),
//        voltureDS(voltureSchema.competenza_consumi).as(competenza_consumi_2),
//        voltureDS(volture).as(etlStage3M2ProposedSchema.volture),
//        voltureDS(volture_hashed).as(volture_hashed)
//      )
//      .withColumn(etlStage3M2ProposedSchema.n_id_fornitura,coalesce(col(n_id_fornitura_1),col(n_id_fornitura_2)))
//      .withColumn(etlStage3M2ProposedSchema.pod,coalesce(col(pod_1),col(pod_2)))
//      .withColumn(etlStage3M2ProposedSchema.competenza_consumi,coalesce(col(competenza_consumi_1),col(competenza_consumi_2)))
//      .withColumn(third_hash,sha2(concat_ws("_",col(second_hash),col(volture_hashed)),256))
//      .select(
//        col(etlStage3M2ProposedSchema.n_id_fornitura),
//        col(etlStage3M2ProposedSchema.pod),
//        col(etlStage3M2ProposedSchema.competenza_consumi),
//        col(etlStage3M2ProposedSchema.misure_orarie),
//        col(etlStage3M2ProposedSchema.misure_mensili),
//        col(etlStage3M2ProposedSchema.misure_non_orarie),
//        col(etlStage3M2ProposedSchema.volture),
//        col(third_hash)
//      )

    val thirdJoinCheckPoint = joinVolture(secondJoin,voltureDS)
    CheckPointUtils.writeCheckpoint(thirdJoinCheckPoint, stage3)

    val thirdJoin = CheckPointUtils.readCheckpoint(spark, stage3)

//    val finalJoin = thirdJoin.join(autolettureDS,
//      thirdJoin(etlStage3M2ProposedSchema.n_id_fornitura) === autolettureDS(autolettureSchema.n_id_fornitura) &&
//        thirdJoin(etlStage3M2ProposedSchema.pod) === autolettureDS(autolettureSchema.pod) &&
//        thirdJoin(etlStage3M2ProposedSchema.competenza_consumi) === autolettureDS(autolettureSchema.competenza_consumi)
//      ,"full_outer")
//      .select(
//        thirdJoin(misureOrarieCSchema.n_id_fornitura).as(n_id_fornitura_1),
//        thirdJoin(misureOrarieCSchema.pod).as(pod_1),
//        thirdJoin(misureOrarieCSchema.competenza_consumi).as(competenza_consumi_1),
//        thirdJoin(etlStage3M2ProposedSchema.misure_orarie).as(etlStage3M2ProposedSchema.misure_orarie),
//        thirdJoin(etlStage3M2ProposedSchema.misure_mensili).as(etlStage3M2ProposedSchema.misure_mensili),
//        thirdJoin(etlStage3M2ProposedSchema.misure_non_orarie).as(etlStage3M2ProposedSchema.misure_non_orarie),
//        thirdJoin(etlStage3M2ProposedSchema.volture).as(etlStage3M2ProposedSchema.volture),
//        thirdJoin(third_hash),
//
//        autolettureDS(autolettureSchema.n_id_fornitura).as(n_id_fornitura_2),
//        autolettureDS(autolettureSchema.pod).as(pod_2),
//        autolettureDS(autolettureSchema.competenza_consumi).as(competenza_consumi_2),
//        autolettureDS(autolettura).as(etlStage3M2ProposedSchema.autoletture),
//        autolettureDS(autolettura_hashed).as(autolettura_hashed)
//      )
//      .withColumn(etlStage3M2ProposedSchema.n_id_fornitura,coalesce(col(n_id_fornitura_1),col(n_id_fornitura_2)))
//      .withColumn(etlStage3M2ProposedSchema.pod,coalesce(col(pod_1),col(pod_2)))
//      .withColumn(etlStage3M2ProposedSchema.competenza_consumi,coalesce(col(competenza_consumi_1),col(competenza_consumi_2)))
//      .withColumn(etlStage3M2ProposedSchema.hash_value,sha2(concat_ws("_",col(etlStage3M2ProposedSchema.n_id_fornitura),col(etlStage3M2ProposedSchema.pod),col(third_hash),col(autolettura_hashed)),256))
//      .select(
//        col(etlStage3M2ProposedSchema.n_id_fornitura),
//        col(etlStage3M2ProposedSchema.pod),
//        col(etlStage3M2ProposedSchema.competenza_consumi),
//        col(etlStage3M2ProposedSchema.misure_orarie),
//        col(etlStage3M2ProposedSchema.misure_mensili),
//        col(etlStage3M2ProposedSchema.misure_non_orarie),
//        col(etlStage3M2ProposedSchema.volture),
//        col(etlStage3M2ProposedSchema.autoletture),
//        col(etlStage3M2ProposedSchema.hash_value)
//      )
//      .withColumn(etlStage3M2Schema.cod_pod, substring(col(etlStage3M2Schema.pod), 11, 2))
//      .distinct()

    val forthJoinCheckPoint = joinAutoletture(thirdJoin,autolettureDS)
    CheckPointUtils.writeCheckpoint(forthJoinCheckPoint, stage4)

    val finalJoin = CheckPointUtils.readCheckpoint(spark, stage4)

    finalJoin
      .withColumn(etlStage3M2ProposedSchema.last_update,lit(currentAnnoMeseGiornoLong).cast(LongType))
      .selectExpr(etlStage3M2ProposedSchema.getValues:_*)
      .as[etlStage3M2ProposedModel]
  }

  //compare data previous run to this one
  def data_compare(stageFinalNowDS: Dataset[etlStage3M2ProposedModel], stageFinalCompareDS: Dataset[etlStage3M2ProposedModel]
                  ): (Dataset[etlStage3M2ProposedModel],Dataset[etlStage3M2ProposedModel],Dataset[etlStage3M2ProposedModel]) = {

    // yyyyMM  in integer format (year and month when the code is running)
    val currentDate = LocalDate.now()
    val currenyAnnoMese = currentDate.format(DateTimeFormatter.ofPattern("yyyyMM")).toInt

    val n_id_fornitura_old = "n_id_fornitura_old"
    val pod_old = "pod_old"
    val competenza_consumi_old = "competenza_consumi_old"
    val hash_value_old = "hash_value_old"
    val last_update_old = "last_update_old"

    def dataCompareCondition =
      (col(etlStage3M2ProposedSchema.hash_value)=!=col(hash_value_old)) || (
        col(n_id_fornitura_old).isNull ||
          col(pod_old).isNull ||
          col(competenza_consumi_old).isNull
        )


    def stageFinalNowDSThisMonth = stageFinalNowDS.filter(col(etlStage3M2ProposedSchema.competenza_consumi) === currenyAnnoMese)

    val stageFinalNowDSPreviousMonth = stageFinalNowDS.filter(col(etlStage3M2Schema.competenza_consumi) =!= currenyAnnoMese)
    val stageFinalCompareDSPreviousMonth = stageFinalCompareDS.filter(col(etlStage3M2ProposedSchema.competenza_consumi) =!= currenyAnnoMese)
      .select(
        col(etlStage3M2ProposedSchema.n_id_fornitura).as(n_id_fornitura_old),
        col(etlStage3M2ProposedSchema.pod).as(pod_old),
        col(etlStage3M2ProposedSchema.competenza_consumi).as(competenza_consumi_old),
        col(etlStage3M2ProposedSchema.hash_value).as(hash_value_old),
        col(etlStage3M2ProposedSchema.last_update).as(last_update_old)
      )

    val JoinHashDataUpdated = stageFinalNowDSPreviousMonth.join(stageFinalCompareDSPreviousMonth,
      stageFinalNowDSPreviousMonth(etlStage3M2ProposedSchema.n_id_fornitura) === stageFinalCompareDSPreviousMonth(n_id_fornitura_old) &&
        stageFinalNowDSPreviousMonth(etlStage3M2ProposedSchema.pod) === stageFinalCompareDSPreviousMonth(pod_old) &&
        stageFinalNowDSPreviousMonth(etlStage3M2ProposedSchema.competenza_consumi) === stageFinalCompareDSPreviousMonth(competenza_consumi_old)
      ,"left")
      .withColumn(etlStage3M2ProposedSchema.last_update,when( dataCompareCondition
        , col(etlStage3M2ProposedSchema.last_update)).otherwise(col(last_update_old)))

      val JoinHashUpdated = JoinHashDataUpdated
      .filter(dataCompareCondition)
      .selectExpr(etlStage3M2ProposedSchema.getValues:_*)
      .as[etlStage3M2ProposedModel]

    val timeZone = EnvironmentMisure.getProperty("spark.app.time_zone")
    val currentAnnoMeseGiornoLong = argumentsUtilitiesExport.convertUtcToLong(timeZone)
    val stageFinalNowDSThisMonthtl = stageFinalNowDSThisMonth.withColumn(etlStage3M2ProposedSchema.last_update,lit(currentAnnoMeseGiornoLong).cast(LongType))
      .selectExpr(etlStage3M2ProposedSchema.getValues:_*)
      .as[etlStage3M2ProposedModel]

    val finalJoinHashDateLastModifiedUpdated = JoinHashDataUpdated.selectExpr(etlStage3M2ProposedSchema.getValues:_*)
      .as[etlStage3M2ProposedModel]

    (stageFinalNowDSThisMonth,JoinHashUpdated,finalJoinHashDateLastModifiedUpdated.unionByName(stageFinalNowDSThisMonthtl))

  }

  //compare data previous run to this one
  def data_compare33M(stageFinalNowDS: Dataset[etlStage3M2ProposedModel], stageFinalCompareDS: Dataset[etlStage3M2ProposedModel]
                  ): (Dataset[etlStage3M2ProposedModel],Dataset[etlStage3M2ProposedModel]) = {

    // yyyyMM  in integer format (year and month when the code is running)
    val currentDate = LocalDate.now()
    val currenyAnnoMese = currentDate.format(DateTimeFormatter.ofPattern("yyyyMM")).toInt

    val n_id_fornitura_old = "n_id_fornitura_old"
    val pod_old = "pod_old"
    val competenza_consumi_old = "competenza_consumi_old"
    val hash_value_old = "hash_value_old"
    val last_update_old = "last_update_old"

    def dataCompareCondition =
      (col(etlStage3M2ProposedSchema.hash_value)=!=col(hash_value_old)) || (
        col(n_id_fornitura_old).isNull ||
          col(pod_old).isNull ||
          col(competenza_consumi_old).isNull
        )

    val stageFinalCompareDSPreviousMonth = stageFinalCompareDS
      .select(
        col(etlStage3M2ProposedSchema.n_id_fornitura).as(n_id_fornitura_old),
        col(etlStage3M2ProposedSchema.pod).as(pod_old),
        col(etlStage3M2ProposedSchema.competenza_consumi).as(competenza_consumi_old),
        col(etlStage3M2ProposedSchema.hash_value).as(hash_value_old),
        col(etlStage3M2ProposedSchema.last_update).as(last_update_old)
      )

    val JoinHashDataUpdated = stageFinalNowDS.join(stageFinalCompareDSPreviousMonth,
      stageFinalNowDS(etlStage3M2ProposedSchema.n_id_fornitura) === stageFinalCompareDSPreviousMonth(n_id_fornitura_old) &&
        stageFinalNowDS(etlStage3M2ProposedSchema.pod) === stageFinalCompareDSPreviousMonth(pod_old) &&
        stageFinalNowDS(etlStage3M2ProposedSchema.competenza_consumi) === stageFinalCompareDSPreviousMonth(competenza_consumi_old)
      ,"left")
      .withColumn(etlStage3M2ProposedSchema.last_update,when( dataCompareCondition
        , col(etlStage3M2ProposedSchema.last_update)).otherwise(col(last_update_old)))


    val JoinHashUpdated = JoinHashDataUpdated
      .filter(dataCompareCondition)
      .selectExpr(etlStage3M2ProposedSchema.getValues:_*)
      .as[etlStage3M2ProposedModel]

    val finalJoinHashDateLastModifiedUpdated = JoinHashDataUpdated.selectExpr(etlStage3M2ProposedSchema.getValues:_*)
      .as[etlStage3M2ProposedModel]

    (JoinHashUpdated,finalJoinHashDateLastModifiedUpdated)

  }

  //cod pod need to be set istead to competenza_consumi
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


  private def joinOrarieMensili(
                                 misureOrarieCDS: Dataset[misureOrarieCRevisitedModel],
                                 misureMensiliCDS: Dataset[misureMensiliCRevisitedModel]
                               ): DataFrame = {

     misureOrarieCDS.join(misureMensiliCDS,
      misureOrarieCDS(misureOrarieCSchema.n_id_fornitura) === misureMensiliCDS(misureMensiliCSchema.n_id_fornitura) &&
        misureOrarieCDS(misureOrarieCSchema.pod) === misureMensiliCDS(misureMensiliCSchema.pod) &&
        misureOrarieCDS(misureOrarieCSchema.competenza_consumi) === misureMensiliCDS(misureMensiliCSchema.competenza_consumi)
      ,"full_outer")
      .select(
        misureOrarieCDS(misureOrarieCSchema.n_id_fornitura).as(n_id_fornitura_1),
        misureOrarieCDS(misureOrarieCSchema.pod).as(pod_1),
        misureOrarieCDS(misureOrarieCSchema.competenza_consumi).as(competenza_consumi_1),
        misureOrarieCDS(misura_oraria_gg).as(etlStage3M2ProposedSchema.misure_orarie),
        misureOrarieCDS(misura_oraria_gg_hashed).as(misura_oraria_gg_hashed),
        misureMensiliCDS(misureMensiliCSchema.n_id_fornitura).as(n_id_fornitura_2),
        misureMensiliCDS(misureMensiliCSchema.pod).as(pod_2),
        misureMensiliCDS(misureMensiliCSchema.competenza_consumi).as(competenza_consumi_2),
        misureMensiliCDS(misura_oraria_mese).as(etlStage3M2ProposedSchema.misure_mensili),
        misureMensiliCDS(misura_oraria_mese_hashed).as(misura_oraria_mese_hashed)
      )
      .withColumn(first_hash,sha2(concat_ws("_",col(misura_oraria_gg_hashed),col(misura_oraria_mese_hashed)),256))
      .withColumn(etlStage3M2ProposedSchema.n_id_fornitura,coalesce(col(n_id_fornitura_1),col(n_id_fornitura_2)))
      .withColumn(etlStage3M2ProposedSchema.pod,coalesce(col(pod_1),col(pod_2)))
      .withColumn(etlStage3M2ProposedSchema.competenza_consumi,coalesce(col(competenza_consumi_1),col(competenza_consumi_2)))
      .select(
        col(etlStage3M2ProposedSchema.n_id_fornitura),
        col(etlStage3M2ProposedSchema.pod),
        col(etlStage3M2ProposedSchema.competenza_consumi),
        col(etlStage3M2ProposedSchema.misure_orarie),
        col(etlStage3M2ProposedSchema.misure_mensili),
        col(first_hash)
      )
  }

  private def joinMisureNonOrarie(firstJoin : DataFrame, misureNonOrarieCDS: Dataset[misureNonOrarieCRevisitedModel]): DataFrame = {
    firstJoin.join(misureNonOrarieCDS,
      firstJoin(etlStage3M2ProposedSchema.n_id_fornitura) === misureNonOrarieCDS(misureNonOrarieCSchema.n_id_fornitura) &&
        firstJoin(etlStage3M2ProposedSchema.pod) === misureNonOrarieCDS(misureNonOrarieCSchema.pod) &&
        firstJoin(etlStage3M2ProposedSchema.competenza_consumi) === misureNonOrarieCDS(misureNonOrarieCSchema.competenza_consumi)
      ,"full_outer")
      .select(
        firstJoin(misureOrarieCSchema.n_id_fornitura).as(n_id_fornitura_1),
        firstJoin(misureOrarieCSchema.pod).as(pod_1),
        firstJoin(misureOrarieCSchema.competenza_consumi).as(competenza_consumi_1),
        firstJoin(etlStage3M2ProposedSchema.misure_orarie).as(etlStage3M2ProposedSchema.misure_orarie),
        firstJoin(etlStage3M2ProposedSchema.misure_mensili).as(etlStage3M2ProposedSchema.misure_mensili),
        firstJoin(first_hash),

        misureNonOrarieCDS(misureNonOrarieCSchema.n_id_fornitura).as(n_id_fornitura_2),
        misureNonOrarieCDS(misureNonOrarieCSchema.pod).as(pod_2),
        misureNonOrarieCDS(misureNonOrarieCSchema.competenza_consumi).as(competenza_consumi_2),
        misureNonOrarieCDS(misura_non_oraria).as(etlStage3M2ProposedSchema.misure_non_orarie),
        misureNonOrarieCDS(misura_non_oraria_hashed).as(misura_non_oraria_hashed)
      )
      .withColumn(etlStage3M2ProposedSchema.n_id_fornitura,coalesce(col(n_id_fornitura_1),col(n_id_fornitura_2)))
      .withColumn(etlStage3M2ProposedSchema.pod,coalesce(col(pod_1),col(pod_2)))
      .withColumn(etlStage3M2ProposedSchema.competenza_consumi,coalesce(col(competenza_consumi_1),col(competenza_consumi_2)))
      .withColumn(second_hash,sha2(concat_ws("_",col(first_hash),col(misura_non_oraria_hashed)),256))
      .select(
        col(etlStage3M2ProposedSchema.n_id_fornitura),
        col(etlStage3M2ProposedSchema.pod),
        col(etlStage3M2ProposedSchema.competenza_consumi),
        col(etlStage3M2ProposedSchema.misure_orarie),
        col(etlStage3M2ProposedSchema.misure_mensili),
        col(etlStage3M2ProposedSchema.misure_non_orarie),
        col(second_hash)
      )

  }

  private def joinVolture(secondJoin : DataFrame, voltureDS: Dataset[voltureRevisitedModel]): DataFrame = {
    secondJoin.join(voltureDS,
      secondJoin(etlStage3M2ProposedSchema.n_id_fornitura) === voltureDS(voltureSchema.n_id_fornitura) &&
        secondJoin(etlStage3M2ProposedSchema.pod) === voltureDS(voltureSchema.pod) &&
        secondJoin(etlStage3M2ProposedSchema.competenza_consumi) === voltureDS(voltureSchema.competenza_consumi)
      ,"full_outer")
      .select(
        secondJoin(misureOrarieCSchema.n_id_fornitura).as(n_id_fornitura_1),
        secondJoin(misureOrarieCSchema.pod).as(pod_1),
        secondJoin(misureOrarieCSchema.competenza_consumi).as(competenza_consumi_1),
        secondJoin(etlStage3M2ProposedSchema.misure_orarie).as(etlStage3M2ProposedSchema.misure_orarie),
        secondJoin(etlStage3M2ProposedSchema.misure_mensili).as(etlStage3M2ProposedSchema.misure_mensili),
        secondJoin(etlStage3M2ProposedSchema.misure_non_orarie).as(etlStage3M2ProposedSchema.misure_non_orarie),
        secondJoin(second_hash),

        voltureDS(voltureSchema.n_id_fornitura).as(n_id_fornitura_2),
        voltureDS(voltureSchema.pod).as(pod_2),
        voltureDS(voltureSchema.competenza_consumi).as(competenza_consumi_2),
        voltureDS(volture).as(etlStage3M2ProposedSchema.volture),
        voltureDS(volture_hashed).as(volture_hashed)
      )
      .withColumn(etlStage3M2ProposedSchema.n_id_fornitura,coalesce(col(n_id_fornitura_1),col(n_id_fornitura_2)))
      .withColumn(etlStage3M2ProposedSchema.pod,coalesce(col(pod_1),col(pod_2)))
      .withColumn(etlStage3M2ProposedSchema.competenza_consumi,coalesce(col(competenza_consumi_1),col(competenza_consumi_2)))
      .withColumn(third_hash,sha2(concat_ws("_",col(second_hash),col(volture_hashed)),256))
      .select(
        col(etlStage3M2ProposedSchema.n_id_fornitura),
        col(etlStage3M2ProposedSchema.pod),
        col(etlStage3M2ProposedSchema.competenza_consumi),
        col(etlStage3M2ProposedSchema.misure_orarie),
        col(etlStage3M2ProposedSchema.misure_mensili),
        col(etlStage3M2ProposedSchema.misure_non_orarie),
        col(etlStage3M2ProposedSchema.volture),
        col(third_hash)
      )
  }

  private def joinAutoletture(thirdJoin : DataFrame, autolettureDS: Dataset[autoletturaRevisitedModel]): DataFrame = {
    thirdJoin.join(autolettureDS,
      thirdJoin(etlStage3M2ProposedSchema.n_id_fornitura) === autolettureDS(autolettureSchema.n_id_fornitura) &&
        thirdJoin(etlStage3M2ProposedSchema.pod) === autolettureDS(autolettureSchema.pod) &&
        thirdJoin(etlStage3M2ProposedSchema.competenza_consumi) === autolettureDS(autolettureSchema.competenza_consumi)
      ,"full_outer")
      .select(
        thirdJoin(misureOrarieCSchema.n_id_fornitura).as(n_id_fornitura_1),
        thirdJoin(misureOrarieCSchema.pod).as(pod_1),
        thirdJoin(misureOrarieCSchema.competenza_consumi).as(competenza_consumi_1),
        thirdJoin(etlStage3M2ProposedSchema.misure_orarie).as(etlStage3M2ProposedSchema.misure_orarie),
        thirdJoin(etlStage3M2ProposedSchema.misure_mensili).as(etlStage3M2ProposedSchema.misure_mensili),
        thirdJoin(etlStage3M2ProposedSchema.misure_non_orarie).as(etlStage3M2ProposedSchema.misure_non_orarie),
        thirdJoin(etlStage3M2ProposedSchema.volture).as(etlStage3M2ProposedSchema.volture),
        thirdJoin(third_hash),

        autolettureDS(autolettureSchema.n_id_fornitura).as(n_id_fornitura_2),
        autolettureDS(autolettureSchema.pod).as(pod_2),
        autolettureDS(autolettureSchema.competenza_consumi).as(competenza_consumi_2),
        autolettureDS(autolettura).as(etlStage3M2ProposedSchema.autoletture),
        autolettureDS(autolettura_hashed).as(autolettura_hashed)
      )
      .withColumn(etlStage3M2ProposedSchema.n_id_fornitura,coalesce(col(n_id_fornitura_1),col(n_id_fornitura_2)))
      .withColumn(etlStage3M2ProposedSchema.pod,coalesce(col(pod_1),col(pod_2)))
      .withColumn(etlStage3M2ProposedSchema.competenza_consumi,coalesce(col(competenza_consumi_1),col(competenza_consumi_2)))
      .withColumn(etlStage3M2ProposedSchema.hash_value,sha2(concat_ws("_",col(etlStage3M2ProposedSchema.n_id_fornitura),col(etlStage3M2ProposedSchema.pod),col(third_hash),col(autolettura_hashed)),256))
      .select(
        col(etlStage3M2ProposedSchema.n_id_fornitura),
        col(etlStage3M2ProposedSchema.pod),
        col(etlStage3M2ProposedSchema.competenza_consumi),
        col(etlStage3M2ProposedSchema.misure_orarie),
        col(etlStage3M2ProposedSchema.misure_mensili),
        col(etlStage3M2ProposedSchema.misure_non_orarie),
        col(etlStage3M2ProposedSchema.volture),
        col(etlStage3M2ProposedSchema.autoletture),
        col(etlStage3M2ProposedSchema.hash_value)
      )
      .withColumn(etlStage3M2Schema.cod_pod, substring(col(etlStage3M2Schema.pod), 11, 2))
      .distinct()
  }

  }
