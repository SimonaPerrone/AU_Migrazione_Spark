package it.eng.au.portale_consumi_ee.trasformations

import it.eng.au.portale_consumi_ee.Main.logger
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.common.utility.functions.costants.DOT
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure.{autolettureModel, etlStage3M2Model, misureMensiliCModel, misureNonOrarieCModel, misureOrarieCModel, registroLoadModel, voltureModel}
import it.eng.au.portale_consumi_ee.schema.misure.{autolettureSchema, etlStage3M2Schema, misureMensiliCSchema, misureNonOrarieCSchema, misureOrarieCSchema, registroLoadSchema, voltureSchema}
import org.apache.spark.sql.catalyst.dsl.expressions.longToLiteral
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, concat, concat_ws, desc, hash, lit, max, substring}
import org.apache.spark.sql.{Dataset, SparkSession}

object stagePhaseTrasformation {
  val spark: SparkSession = EnvironmentMisure.getSpark

  import spark.implicits._
  val fornitura_pod = "fornitura_pod"
  val misura_oraria_gg = "misure_orarie"
  val misura_oraria_mese = "misure_mensili"
  val misura_non_oraria = "misure_non_orarie"
  val volture  = "volture"
  val autolettura = "autoletture"
  val giorno = "giorno"
  val maxG = "maxG"
  val tabella = "tabella"
  val hash_value = "hash_value"

  val now= "now"
  val previous_week = "previous_week"
  val hash_diff = "hash_diff"
  val has_diff = "has_diff"
  val new_forniture_pod = "new_forniture_pod"
  val is_new = "is_new"
  val filterOp = "filterOp"

  // calcolo stage data on this run
  def calcolo_stage(
                     autolettureDS :Dataset[autolettureModel],
                     misureMensiliCDS : Dataset[misureMensiliCModel],
                     misureNonOrarieCDS : Dataset[misureNonOrarieCModel],
                     misureOrarieCDS: Dataset[misureOrarieCModel],
                     voltureDS: Dataset[voltureModel]
                 ): Dataset[etlStage3M2Model] = {

    // misure_orarie_c computation
    logger.info(s"Inizio trasformazion  misure.misure_orarie_c")
    // Apply the transformations
    val misureOrarieCTrasformed = misureOrarieCDS
      .withColumn(
        fornitura_pod,
        concat(col(misureOrarieCSchema.n_id_fornitura), lit("/b"), col(misureOrarieCSchema.pod))
      )
      .withColumn(
        misura_oraria_gg,
        concat_ws(
          "/b",
          coalesce(col(misureOrarieCSchema.competenza_consumi), lit("")),
          coalesce(col(misureOrarieCSchema.consumo_giornaliero_gg), lit("")),
          coalesce(col(misureOrarieCSchema.lettura_giornaliero_f1), lit("")),
          coalesce(col(misureOrarieCSchema.lettura_giornaliero_f2), lit("")),
          coalesce(col(misureOrarieCSchema.lettura_giornaliero_f3), lit("")),
          coalesce(col(misureOrarieCSchema.lettura_giornaliero_f4), lit("")),
          coalesce(col(misureOrarieCSchema.lettura_giornaliero_f5), lit("")),
          coalesce(col(misureOrarieCSchema.lettura_giornaliero_f6), lit("")),
          coalesce(col(misureOrarieCSchema.delta_misure_f1), lit("")),
          coalesce(col(misureOrarieCSchema.delta_misure_f2), lit("")),
          coalesce(col(misureOrarieCSchema.delta_misure_f3), lit("")),
          coalesce(col(misureOrarieCSchema.delta_misure_f4), lit("")),
          coalesce(col(misureOrarieCSchema.delta_misure_f5), lit("")),
          coalesce(col(misureOrarieCSchema.delta_misure_f6), lit("")),
          coalesce(col(misureOrarieCSchema.giorno), lit("")),
          coalesce(col(misureOrarieCSchema.potenza_max_erogata), lit("")),
          coalesce(col(misureOrarieCSchema.tipo_flusso), lit("")),
          coalesce(col(misureOrarieCSchema.data_lettura), lit(""))
        )
      )
      .withColumn(misura_oraria_mese, lit(""))
      .withColumn(misura_non_oraria, lit(""))
      .withColumn(volture, lit(""))
      .withColumn(autolettura, lit(""))
      .withColumn(tabella, lit(1))
      .withColumn(hash_value, hash(col(misura_oraria_gg)))
      .select(
        col(fornitura_pod),
        col(misura_oraria_gg),
        col(misura_oraria_mese),
        col(misura_non_oraria),
        col(volture),
        col(autolettura),
        col(misureOrarieCSchema.competenza_consumi),
        col(misureOrarieCSchema.giorno),
        col(misureOrarieCSchema.pod),
        col(tabella),
        col(hash_value)
      )
    logger.info(s"fine trasformazion  misure.misure_orarie_c")

    // misure_mensili_c trasformation
    logger.info(s"Inizio trasformazion  misure.misure_mensili_c")
    // Define a window specification for calculating max(giorno)
    val windowMaxGiorno = Window.partitionBy(misureMensiliCSchema.n_id_fornitura, misureMensiliCSchema.pod, misureMensiliCSchema.competenza_consumi)

    val misureMensiliCTrasfornmation = misureMensiliCDS
      .withColumn(
        fornitura_pod,
        concat(col(misureMensiliCSchema.n_id_fornitura), lit("/b"), col(misureMensiliCSchema.pod))
      )
      .withColumn(misura_oraria_gg, lit(""))
      .withColumn(
        misura_oraria_mese,
        concat_ws(
          "/b",
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
      .withColumn(misura_non_oraria, lit(""))
      .withColumn(volture, lit(""))
      .withColumn(autolettura, lit(""))
      .withColumn(giorno, substring(col(misureMensiliCSchema.data_lettura), 7, 2).cast("int"))
      .withColumn(
        maxG,
        max(substring(col(misureMensiliCSchema.data_lettura), 7, 2).cast("int")).over(windowMaxGiorno)
      )
      .filter(col(giorno) === col(maxG))
      .withColumn(tabella, lit(2))
      .withColumn(hash_value, hash(col(misura_oraria_mese)))
      .select(
        col(fornitura_pod),
        col(misura_oraria_gg),
        col(misura_oraria_mese),
        col(misura_non_oraria),
        col(volture),
        col(autolettura),
        col(misureMensiliCSchema.competenza_consumi),
        col(giorno),
        col(misureMensiliCSchema.pod),
        col(tabella),
        col(hash_value)
      )
      //todo operation necessary to ensure that each row for this keys are unique
      // ideally one day
//      .groupBy(
//        col(fornitura_pod),
//        col(misure_orarie),
//        col(misure_mensili),
//        col(misure_non_orarie),
//        col(volture),
//        col(autoletture),
//        col(misureMensiliCSchema.competenza_consumi),
//        col(giorno),
//        col(misureMensiliCSchema.pod)
//      )
//      .agg()
    //todo it is okey this step?
      .distinct()
    logger.info(s"Fine trasformazion  misure.misure_mensili_c")

//  misure_non_orarie_c trasformation
    logger.info(s"Inizio trasformazion  misure.misure_non_orarie_c")

    val misureNonOrarieTrasformd = misureNonOrarieCDS
      .withColumn(
        fornitura_pod,
        concat(col(misureNonOrarieCSchema.n_id_fornitura), lit("/b"), col(misureNonOrarieCSchema.pod))
      )
      .withColumn("misure_orarie", lit(""))
      .withColumn("misure_mensili", lit(""))
      .withColumn(
        misura_non_oraria,
        concat_ws(
          "/b",
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
      .withColumn(volture, lit(""))
      .withColumn(autolettura, lit(""))
      .withColumn(giorno, lit(1))
      .withColumn(tabella, lit(3))
      .withColumn(hash_value, hash(col(misura_non_oraria)))
      .select(
        col(fornitura_pod),
        col(misura_oraria_gg),
        col(misura_oraria_mese),
        col(misura_non_oraria),
        col(volture),
        col(autolettura),
        col(misureNonOrarieCSchema.competenza_consumi),
        col(giorno),
        col(misureNonOrarieCSchema.pod),
        col(tabella),
        col(hash_value)
      )
    logger.info(s"fine trasformazion  misure.misure_non_orarie_c")

    // volture trasformation
    logger.info(s"inizio  trasformazion  misure.volture")
    val voltureTrasformed = voltureDS
      .withColumn(
        fornitura_pod,
        concat(col(voltureSchema.n_id_fornitura), lit("/b"), col(voltureSchema.pod))
      )
      .withColumn(misura_oraria_gg, lit(""))
      .withColumn(misura_oraria_mese, lit(""))
      .withColumn(misura_non_oraria, lit(""))
      .withColumn(
        volture,
        concat_ws(
          "/b",
          coalesce(col(voltureSchema.competenza_consumi), lit("")),
          coalesce(col(voltureSchema.data_lettura), lit("")),
          coalesce(col(voltureSchema.lettura_misura_monoraria), lit("")),
          coalesce(col(voltureSchema.lettura_misura_f1), lit("")),
          coalesce(col(voltureSchema.lettura_misura_f2), lit("")),
          coalesce(col(voltureSchema.lettura_misura_f3), lit("")),
          coalesce(col(voltureSchema.lettura_misura_f4), lit("")),
          coalesce(col(voltureSchema.lettura_misura_f5), lit("")),
          coalesce(col(voltureSchema.lettura_misura_f6), lit("")),
          coalesce(col(voltureSchema.tipo_flusso2), lit(" "))
        )
      )
      .withColumn(autolettura, lit(""))
      .withColumn(giorno, lit(1))
      .withColumn(tabella, lit(4))
      .withColumn(hash_value, hash(col(volture)))
      .select(
        col(fornitura_pod),
        col(misura_oraria_gg),
        col(misura_oraria_mese),
        col(misura_non_oraria),
        col(volture),
        col(autolettura),
        col(voltureSchema.competenza_consumi),
        col(giorno),
        col(voltureSchema.pod),
        col(tabella),
        col(hash_value)
      )
    logger.info(s"fine trasformazion  misure.volture")

    // autoletture Trasformation
    // Transform the DataFrame to match the SQL query logic
    logger.info(s"inizio  trasformazion  misure.autoletture")

    val autolettureTrasformaiton = autolettureDS
      .withColumn(
        fornitura_pod,
        concat(col(autolettureSchema.n_id_fornitura), lit("/b"), col(autolettureSchema.pod))
      )
      .withColumn(misura_oraria_gg, lit(""))
      .withColumn(misura_oraria_mese, lit(""))
      .withColumn(misura_non_oraria, lit(""))
      .withColumn(volture, lit(""))
      .withColumn(
        autolettura,
        concat_ws(
          "/b",
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
      .withColumn(giorno, lit(1))
      .withColumn(tabella, lit(5))
      .withColumn(hash_value, hash(col(autolettura)))
      .select(
        col(fornitura_pod),
        col(misura_oraria_gg),
        col(misura_oraria_mese),
        col(misura_non_oraria),
        col(volture),
        col(autolettura),
        col(autolettureSchema.competenza_consumi),
        col(giorno),
        col(autolettureSchema.pod),
        col(tabella),
        col(hash_value)
      )
    logger.info(s"fine trasformazion  misure.autoletture")

    val stageFinalDF = misureOrarieCTrasformed.unionByName(misureMensiliCTrasfornmation).unionByName(misureNonOrarieTrasformd)
      .unionByName(voltureTrasformed).unionByName(autolettureTrasformaiton)
      .withColumn(etlStage3M2Schema.cod_pod, substring(col(etlStage3M2Schema.pod), 11, 3))
      .as[etlStage3M2Model]

    //todo to remove
    stageFinalDF.printSchema()

    stageFinalDF
  }

  //compare data previous run to this one
  def data_compare(stageFinalNowDS: Dataset[etlStage3M2Model], stageFinalCompareDS: Dataset[etlStage3M2Model]
                  ): Dataset[etlStage3M2Model] = {

    //logic for table with monthly data
    val stageFinalNowMonthlyDS = stageFinalNowDS.filter(col(etlStage3M2Schema.tabella) =!= lit(1))

    val stageFinalCompareMonthlyDS = stageFinalCompareDS.filter(col(etlStage3M2Schema.tabella) =!= lit(1))

    // Select all columns from `now` dataset
    val nowMonthlyColumns = stageFinalNowMonthlyDS.columns.map(colName => col(s"$now.$colName"))

    val joinedMontlyData = stageFinalNowMonthlyDS.as(now).join(stageFinalCompareMonthlyDS.as(previous_week)
      , col(now +DOT + etlStage3M2Schema.fornitura_pod) === col(previous_week +DOT + etlStage3M2Schema.fornitura_pod ) &&
        col(now +DOT + etlStage3M2Schema.tabella) === col(previous_week +DOT + etlStage3M2Schema.tabella ) &&
        col(now  +DOT+ etlStage3M2Schema.competenza_consumi) === col(previous_week  +DOT+ etlStage3M2Schema.competenza_consumi ) &&
        col(now +DOT + etlStage3M2Schema.giorno) === col(previous_week +DOT + etlStage3M2Schema.giorno )
      ,"left")
      .filter((col(previous_week +DOT + etlStage3M2Schema.fornitura_pod).isNull )  ||
        (col(previous_week +DOT + etlStage3M2Schema.fornitura_pod).isNotNull &&
          col(now +DOT + etlStage3M2Schema.hash_value) =!= col(previous_week +DOT + etlStage3M2Schema.hash_value)
          )
      ).select(nowMonthlyColumns:_*)

    //todo to remove
//    joinedMontlyData.printSchema()
//    joinedMontlyData.show()

    //logic for table with daily data
    val stageFinalNowDailyDS = stageFinalNowDS.filter(col(etlStage3M2Schema.tabella) === lit(1))

    val stageFinalCompareDailyDS = stageFinalCompareDS.filter(col(etlStage3M2Schema.tabella) === lit(1))

    // Select all columns from `now` dataset
    val nowDailyColumns = stageFinalNowDailyDS.columns.map(colName => col(s"$now.$colName"))


    val joinedDailyData = stageFinalNowDailyDS.as(now).join(stageFinalCompareDailyDS.as(previous_week)
      , col(now+ DOT + etlStage3M2Schema.fornitura_pod) === col(previous_week+ DOT + etlStage3M2Schema.fornitura_pod ) &&
        col(now+ DOT + etlStage3M2Schema.tabella) === col(previous_week+ DOT + etlStage3M2Schema.tabella ) &&
        col(now+ DOT + etlStage3M2Schema.competenza_consumi) === col(previous_week+ DOT + etlStage3M2Schema.competenza_consumi ) &&
        col(now+ DOT + etlStage3M2Schema.giorno) === col(previous_week+ DOT + etlStage3M2Schema.giorno )
      ,"left")
      .select(nowDailyColumns :+
        col(s"$previous_week.${etlStage3M2Schema.fornitura_pod}") :+
        col(s"$previous_week.${etlStage3M2Schema.hash_value}")
        : _*)

     //update or new data data
     val  withHashDifference  = joinedDailyData
       //boolean value
       .withColumn(
        hash_diff,
         col(now+ DOT + etlStage3M2Schema.hash_value) =!= col(previous_week+ DOT + etlStage3M2Schema.hash_value )
     )
       .withColumn(
         new_forniture_pod,
         col(previous_week + DOT + etlStage3M2Schema.fornitura_pod ).isNull
       )

    // Group by `fornitura_pod` and `competenze_consumi` of new df and check if any row has a hash difference
    val filteredData = withHashDifference
      .groupBy(now+ DOT + fornitura_pod, now+ DOT + etlStage3M2Schema.competenza_consumi)
      .agg(
        max(hash_diff).as(has_diff), // `has_diff` will be true if any row has a difference
        max(new_forniture_pod).as(is_new) // `is_new` will be false forniture_pod is new for this competenza_consumi respect to previous footprint
      )
      .filter(col(has_diff)|| col(is_new)) // Keep only groups with at least one difference
      .select(now+ DOT + fornitura_pod, now+ DOT + etlStage3M2Schema.competenza_consumi) // Select only group identifiers
      .distinct()

    // Join back with the original data to preserve all rows in matching groups
    val resultDailtDataUpdate = joinedDailyData.join(
      filteredData.as(filterOp),
      filteredData(fornitura_pod) === col (filterOp+ DOT +fornitura_pod)  &&
        filteredData(etlStage3M2Schema.competenza_consumi) === col (filterOp+ DOT + etlStage3M2Schema.competenza_consumi)
      ,
      "inner"
    ).select(nowDailyColumns : _*)

    //todo to remove
//    resultDailtDataUpdate.printSchema()
//    resultDailtDataUpdate.show()

  val finalResult = resultDailtDataUpdate.union(joinedMontlyData)

    finalResult.as[etlStage3M2Model]
  }

  def controlloEtlPrecedente(etlStage3m2: Dataset[etlStage3M2Model], registroLoad: Dataset[registroLoadModel]
                  ): Boolean = {

    val totCodPodStage = etlStage3m2.select(col(etlStage3M2Schema.competenza_consumi)).distinct().count()
    val totCodPodLoad = registroLoad.select(col(registroLoadSchema.competenza_consumi)).distinct().count()

    var dfCodPodStage: Option[Int] = None
    var dfCodPodLoad: Option[Int] = None
    var isEtlPrecedenteOk=true

    // ottengo la partizione "più grande" di stage
    if (totCodPodStage > 0) {
      dfCodPodStage = Some(etlStage3m2.select(col(etlStage3M2Schema.competenza_consumi)).distinct().orderBy(desc(etlStage3M2Schema.competenza_consumi))
        .collect()(0).getInt(0))
    }
    //ottengo la partizione più grande di registro
    if (totCodPodLoad >0) {
      dfCodPodLoad = Some(registroLoad.select(col(etlStage3M2Schema.competenza_consumi)).distinct().orderBy(desc(registroLoadSchema.competenza_consumi))
        .collect()(0).getInt(0))
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

}
