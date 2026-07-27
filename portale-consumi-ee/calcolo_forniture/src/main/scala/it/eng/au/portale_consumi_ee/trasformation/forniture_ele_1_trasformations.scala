package it.eng.au.portale_consumi_ee.trasformation

import it.eng.au.portale_consumi_ee.Main.logger
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.model.mongodbs.SwitchModel
import it.eng.au.portale_consumi_ee.model.swtch.SwtchPrtSePModel
import it.eng.au.portale_consumi_ee.model.userappl.UserapplT001AppPrtPratichePModel
import it.eng.au.portale_consumi_ee.schema.mongodbs.SwitchSchema
import it.eng.au.portale_consumi_ee.schema.swtch.SwtchPrtSePSchema
import it.eng.au.portale_consumi_ee.schema.userappl.UserapplT001AppPrtPratichePSchema
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, SparkSession}

// spark implementation of hql_forniture_ele_1_human_readble.sql
object forniture_ele_1_trasformations {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  val data_switch = "data_switch"
  val m_data_contratto = "m_data_contratto"
  val switch_1 = "switch_1"
  val switch_2 = "switch_2"
  val t001_app_prt_pratiche = "t001_app_prt_pratiche"

  def calcolo_switch(dsSwtchPrtSeP: Dataset[SwtchPrtSePModel],
                        dsUserapplT001AppPrtPraticheP:Dataset[UserapplT001AppPrtPratichePModel]
                       ): Dataset[SwitchModel] = {


    // Apply transformations
    val dfSwtchPrtSePAggregated = dsSwtchPrtSeP
      .filter(
        (coalesce(col(SwtchPrtSePSchema.b_ammissibile), lit("")) === "Y") &&
          (coalesce(col(SwtchPrtSePSchema.b_invalidata), lit("")) =!= "Y")
      )
      .groupBy(SwtchPrtSePSchema.t_codice_pod)
      .agg(
        max(SwtchPrtSePSchema.d_data_decorrenza).as(data_switch),
        max(SwtchPrtSePSchema.d_data_contratto).as(m_data_contratto)
      )
      .distinct()

    val switch_tmp = dsSwtchPrtSeP.as(switch_1).join(dfSwtchPrtSePAggregated.as(switch_2),
      dsSwtchPrtSeP(SwtchPrtSePSchema.t_codice_pod) === dfSwtchPrtSePAggregated(SwtchPrtSePSchema.t_codice_pod) &&
        dsSwtchPrtSeP(SwtchPrtSePSchema.d_data_decorrenza) === dfSwtchPrtSePAggregated(data_switch) &&
        dsSwtchPrtSeP(SwtchPrtSePSchema.d_data_contratto) === dfSwtchPrtSePAggregated(m_data_contratto)
      ,"inner")
      .join(dsUserapplT001AppPrtPraticheP.as(t001_app_prt_pratiche)
        ,dsSwtchPrtSeP(SwtchPrtSePSchema.n_id_pratica) === dsUserapplT001AppPrtPraticheP(UserapplT001AppPrtPratichePSchema.n_id_pratica)
        ,"left")
      .select(
        dsSwtchPrtSeP(SwtchPrtSePSchema.t_codice_pod),
        dsSwtchPrtSeP(SwtchPrtSePSchema.d_data_decorrenza),
        dsSwtchPrtSeP(SwtchPrtSePSchema.n_id_pratica),
        dsUserapplT001AppPrtPraticheP(UserapplT001AppPrtPratichePSchema.t_stato),
        dsSwtchPrtSeP(SwtchPrtSePSchema.n_id_cliente_rcu)
      )

    val switch = switch_tmp.select(
      substring(col(SwtchPrtSePSchema.t_codice_pod), 1, 14).as(SwitchSchema.t_codice_pod),
      when(coalesce(col(SwtchPrtSePSchema.d_data_decorrenza), lit("")) === "",
        lit(19700101).cast(LongType)
      ).otherwise(
        concat(
          substring(col(SwtchPrtSePSchema.d_data_decorrenza), 1, 4),
          substring(col(SwtchPrtSePSchema.d_data_decorrenza), 6, 2),
          substring(col(SwtchPrtSePSchema.d_data_decorrenza), 9, 2)
        ).cast(LongType)
      ).as(SwitchSchema.data_switch),
      col(SwitchSchema.n_id_pratica),
      when(
        col(UserapplT001AppPrtPratichePSchema.t_stato).isin("INCORSO", "IN CORSO"),
        lit("true")
      ).otherwise(lit("false")).as(SwitchSchema.switching_in_corso),
      col(SwtchPrtSePSchema.n_id_cliente_rcu).as(SwitchSchema.n_id_cliente)
    ).selectExpr(SwitchSchema.getValues:_*)
      .as[SwitchModel]

    switch
  }


}
