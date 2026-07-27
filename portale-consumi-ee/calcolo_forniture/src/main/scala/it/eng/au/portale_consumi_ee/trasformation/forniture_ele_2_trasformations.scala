package it.eng.au.portale_consumi_ee.trasformation

import it.eng.au.portale_consumi_ee.Main.logger
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.common.utility.functions.costants
import it.eng.au.portale_consumi_ee.model.mongodbs.{GdmModel, SwitchModel}
import it.eng.au.portale_consumi_ee.model.rcu.{RcuPodMisurePModel, RcuPodPModel, RcuPodTecnPModel}
import it.eng.au.portale_consumi_ee.model.rcus.RcusPodtecnPModel
import it.eng.au.portale_consumi_ee.schema.mongodbs.GdmSchema
import it.eng.au.portale_consumi_ee.schema.rcu.{RcuPodMisurePSchema, RcuPodPSchema, RcuPodTecnPSchema}
import it.eng.au.portale_consumi_ee.schema.rcus.RcusPodtecnPSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, TimestampType}
import org.apache.spark.sql.{Dataset, SparkSession}

// spark implementation of hql_forniture_ele_1_human_readble.sql
object forniture_ele_2_trasformations {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  val rcu_pod_tecn_out = "rcu_pod_tecn_out"
  val d_inst_misurator_att = "d_inst_misurator_att"
  val rcus_podtecn = "rcus_podtecn"
  val rcu_pod_misure = "rcu_pod_misure"
  val current_anno_mese = "current_anno_mese"
  val trattamento = "trattamento"
  val pods= "pods"
  val codice_pod = "codice_pod"
  val d_oper_misurator_att = "d_oper_misurator_att"
  val cambio_gdm = "cambio_gdm"
  val data_cambio_GDM = "data_cambio_GDM"
  val data_cambio_GDM_str = "data_cambio_GDM_str"
  val stato_misuratore_2g = "stato_misuratore_2g"
  val anno_start_misure_orarie= "anno_start_misure_orarie"
  val mese_start_misure_orarie = "mese_start_misure_orarie"
  val d_oper_misurator_att_str = "d_oper_misurator_att_str"

  // see hql_forniture_ele_2_human_readble.sql
  def calcolo_gdm(dsRcuPodMisureP: Dataset[RcuPodMisurePModel],
                     dsRcuPodP:Dataset[RcuPodPModel],
                     dsRcuPodTecnP: Dataset[RcuPodTecnPModel],
                     dsRcusPodTecnP: Dataset[RcusPodtecnPModel]
                       ): Dataset[GdmModel] = {


    //first part of the join, definition of rcu_pod_tecn_out
    val dfRcuPodTecnOut = dsRcuPodTecnP
      .groupBy(
        col(RcuPodTecnPSchema.n_id_pod),
        col(RcuPodTecnPSchema.n_potenza_disponibile),
        col(RcuPodTecnPSchema.n_potenza_impegnata),
        col(RcuPodTecnPSchema.n_tensione),
        col(RcuPodTecnPSchema.t_tipo_misuratore),
        col(RcuPodTecnPSchema.t_mat_misuratore_att),
        col(RcuPodTecnPSchema.d_oper_misurator_att)
      )
      .agg(
        max(col(RcuPodTecnPSchema.d_inst_misurator_att)).alias(d_inst_misurator_att)
      ).as(rcu_pod_tecn_out)
      .persist()

    //second part of the join, definition of rcus_podtecn
    val dfRcusPodtecn = dsRcusPodTecnP.join(dsRcuPodTecnP,
      dsRcusPodTecnP(RcusPodtecnPSchema.n_id_pod) === dsRcuPodTecnP(RcuPodTecnPSchema.n_id_pod)
    )
      .filter(
        dsRcusPodTecnP(RcusPodtecnPSchema.t_mat_misuratore_att).isNotNull &&
          dsRcusPodTecnP(RcusPodtecnPSchema.t_mat_misuratore_att) =!= dsRcuPodTecnP(RcuPodTecnPSchema.t_mat_misuratore_att)
      )
      .select(dsRcusPodTecnP("*"))
      .as(rcus_podtecn)
      .persist()

    // definition of dati_trattamento
    // first of all, definition of rcu_pod_misure
    val dfRcuPodMisure = dsRcuPodMisureP.select(
      col(RcuPodMisurePSchema.n_id_pod),
      concat(
        substring(col(RcuPodMisurePSchema.d_anno_mese), 1, 4),
        substring(col(RcuPodMisurePSchema.d_anno_mese), 6, 2),
        substring(col(RcuPodMisurePSchema.d_anno_mese), 9, 2)
      ).cast(LongType).alias(RcuPodMisurePSchema.d_anno_mese),
      col(RcuPodMisurePSchema.t_trattamento_succ),
      col(RcuPodMisurePSchema.t_trattamento),
      concat(
        year(costants.currentDate),
        lpad(month(costants.currentDate).cast(StringType), 2, "0"),
        lit("01")
      ).cast(LongType).alias(current_anno_mese)
    )

    // Define dati_trattamento
    val datiTrattamento = dfRcuPodMisure.select(
      col(RcuPodMisurePSchema.n_id_pod),
      col(RcuPodMisurePSchema.d_anno_mese),
      when(col(RcuPodMisurePSchema.d_anno_mese) < col(current_anno_mese),
        coalesce(
          when(col(RcuPodMisurePSchema.t_trattamento_succ).isNull || col(RcuPodMisurePSchema.t_trattamento_succ) === "", null)
            .otherwise(col(RcuPodMisurePSchema.t_trattamento_succ)),
          col(RcuPodMisurePSchema.t_trattamento)
        )
      )
        .when(col(RcuPodMisurePSchema.d_anno_mese) >= col(current_anno_mese),
          coalesce(
            when(col(RcuPodMisurePSchema.t_trattamento).isNull || col(RcuPodMisurePSchema.t_trattamento) === "", null)
              .otherwise(col(RcuPodMisurePSchema.t_trattamento)),
            col(RcuPodMisurePSchema.t_trattamento_succ)
          )
        )
        .otherwise(null)
        .alias(trattamento)
    ).persist()

    val gdmJoin = dfRcuPodTecnOut.join(dfRcusPodtecn,dfRcuPodTecnOut(RcuPodTecnPSchema.n_id_pod)===dfRcusPodtecn(RcusPodtecnPSchema.n_id_pod),"left")
                  .join(datiTrattamento,dfRcuPodTecnOut(RcuPodTecnPSchema.n_id_pod)===datiTrattamento(RcuPodMisurePSchema.n_id_pod),"left")
                  .join(dsRcuPodP.as(pods),dfRcuPodTecnOut(RcuPodTecnPSchema.n_id_pod)===dsRcuPodP(RcuPodPSchema.n_id_pod),"inner")

    dfRcuPodTecnOut.unpersist()
    dfRcusPodtecn.unpersist()
    datiTrattamento.unpersist()

    // Final select statement
    val gdm = gdmJoin.select(
      dfRcuPodTecnOut(RcuPodTecnPSchema.n_id_pod),
      substring(dsRcuPodP(RcuPodPSchema.t_codice_pod), 1, 14).as(codice_pod),
      dfRcuPodTecnOut(RcuPodTecnPSchema.n_potenza_disponibile),
      dfRcuPodTecnOut(RcuPodTecnPSchema.n_potenza_impegnata),
      dfRcuPodTecnOut(RcuPodTecnPSchema.n_tensione),
      dfRcuPodTecnOut(RcuPodTecnPSchema.t_tipo_misuratore),
      when(dfRcuPodTecnOut(RcuPodTecnPSchema.d_oper_misurator_att).isNull || dfRcuPodTecnOut(RcuPodTecnPSchema.d_oper_misurator_att) === "", lit(19700101).cast(LongType))
        .otherwise(
          concat(
            substring(dfRcuPodTecnOut(RcuPodTecnPSchema.d_oper_misurator_att), 1, 4),
            substring(dfRcuPodTecnOut(RcuPodTecnPSchema.d_oper_misurator_att), 6, 2),
            substring(dfRcuPodTecnOut(RcuPodTecnPSchema.d_oper_misurator_att), 9, 2)
          ).cast(LongType)
        )
        .alias(d_oper_misurator_att),
      coalesce(dfRcuPodTecnOut(RcuPodTecnPSchema.d_oper_misurator_att),lit("")).alias(d_oper_misurator_att_str)
      ,
        when(dfRcusPodtecn(RcusPodtecnPSchema.n_id_pod).isNull, "").otherwise("SI").alias(cambio_gdm), // Define the cambio_GDM column
          when(dfRcusPodtecn(RcusPodtecnPSchema.n_id_pod).isNull, lit(19700101).cast(LongType))
          .otherwise(
            concat(
              substring(dfRcuPodTecnOut(RcuPodTecnPSchema.d_inst_misurator_att), 1, 4),
              substring(dfRcuPodTecnOut(RcuPodTecnPSchema.d_inst_misurator_att), 6, 2),
              substring(dfRcuPodTecnOut(RcuPodTecnPSchema.d_inst_misurator_att), 9, 2)
            ).cast(LongType)
          ).alias(data_cambio_GDM), // Define the new column
        when(dfRcusPodtecn(RcusPodtecnPSchema.n_id_pod).isNull, lit(""))
        .otherwise(
          // Use concat and substring for d_inst_misurator_att
          coalesce(
            concat(
              substring(dfRcuPodTecnOut(RcuPodTecnPSchema.d_inst_misurator_att), 1, 4),  // Year
              substring(dfRcuPodTecnOut(RcuPodTecnPSchema.d_inst_misurator_att), 6, 2),  // Month
              substring(dfRcuPodTecnOut(RcuPodTecnPSchema.d_inst_misurator_att), 9, 2)   // Day
            ),
            lit("") // In case of null, return empty string
          )
        ).alias(data_cambio_GDM_str), // Define the new column
      datiTrattamento(trattamento),
      lit("").as(stato_misuratore_2g),
      dfRcuPodTecnOut(RcusPodtecnPSchema.t_mat_misuratore_att),
      // Create d_inst_misurator_att as BIGINT by concatenating year, month, and day from d_inst_misurator_att
      concat(
        substring(dfRcuPodTecnOut(RcusPodtecnPSchema.d_inst_misurator_att), 1, 4),  // Year part
        substring(dfRcuPodTecnOut(RcusPodtecnPSchema.d_inst_misurator_att), 6, 2),  // Month part
        substring(dfRcuPodTecnOut(RcusPodtecnPSchema.d_inst_misurator_att), 9, 2)   // Day part
      ).cast(LongType).alias(d_inst_misurator_att), // Cast as BIGINT
      // Calculate anno_start_misure_orarie by adding 370 days to d_oper_misurator_att and extracting the year
      year(date_add(dfRcuPodTecnOut(RcusPodtecnPSchema.d_oper_misurator_att).cast(TimestampType), 370))
        .alias(anno_start_misure_orarie), // Alias the new column
      // Calculate mese_start_misure_orarie
      when(
        (month(date_add(dfRcuPodTecnOut(RcusPodtecnPSchema.d_oper_misurator_att).cast(TimestampType), 365)) + 1) > 12,
        lit(1)  // If month exceeds 12, set to 1 (January)
      )
        .otherwise(month(date_add(dfRcuPodTecnOut(RcusPodtecnPSchema.d_oper_misurator_att).cast(TimestampType), 365)) + 1)
        .alias(mese_start_misure_orarie) // Alias the new column

    )
      .dropDuplicates(RcuPodTecnPSchema.n_id_pod)

    gdm.selectExpr(GdmSchema.getValues:_*)
      .as[GdmModel]

  }

}
