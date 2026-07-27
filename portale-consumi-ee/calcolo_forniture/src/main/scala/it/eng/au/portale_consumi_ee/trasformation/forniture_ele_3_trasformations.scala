package it.eng.au.portale_consumi_ee.trasformation

import it.eng.au.portale_consumi_ee.Main.logger
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.common.utility.functions.sqlToSparkUtilitties
import it.eng.au.portale_consumi_ee.model.mongodbs.RcuPodDistrModel
import it.eng.au.portale_consumi_ee.model.rcu.{RcuAziendaPModel, RcuPodDistrPModel}
import it.eng.au.portale_consumi_ee.schema.mongodbs.RcuPodDistrSchema
import it.eng.au.portale_consumi_ee.schema.rcu.{RcuAziendaPSchema, RcuPodDistrPSchema}
import org.apache.spark.sql.functions.{broadcast, coalesce, col, lit}
import org.apache.spark.sql.{Dataset, SparkSession}

// spark implementation of hql_forniture_ele_3_human_readble.sql
object forniture_ele_3_trasformations {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  val n_id_azienda_numeric = "n_id_azienda_numeric"
  val t_rag_soc = "T_rag_soc"
  val rcu_azienda = "rcu_azienda"
  val rcu_pod_distr = "rcu_pod_distr"

  // see hql_forniture_ele_3_human_readble.sql
  def calcolo_RcuPodDistr(
                         dsRcuPodDistrP : Dataset[RcuPodDistrPModel],
                         dsRcuAziendaP: Dataset[RcuAziendaPModel]
                       ): Dataset[RcuPodDistrModel] = {

    //definition RCU_AZIENDA
    val dfRcuAzienda = dsRcuAziendaP
      .withColumn(n_id_azienda_numeric,sqlToSparkUtilitties.isNumericUDF(col(RcuAziendaPSchema.n_id_azienda)))
      .filter(col(n_id_azienda_numeric ) === true)
      .select(
        col(RcuAziendaPSchema.n_id_azienda),
        col(RcuAziendaPSchema.t_rag_soc).alias(t_rag_soc),
        col(RcuAziendaPSchema.t_piva)
      )

    val rcuPodDistrJoin = dsRcuPodDistrP.as(rcu_pod_distr).join(broadcast(dfRcuAzienda).as(rcu_azienda)
      ,dfRcuAzienda(RcuAziendaPSchema.n_id_azienda) === coalesce(dsRcuPodDistrP(RcuPodDistrPSchema.n_id_distr), lit(""))
      ,"inner"
    )

    val rcuPodDistr = rcuPodDistrJoin.select(
      dsRcuPodDistrP(RcuPodDistrPSchema.n_id_pod),
      dfRcuAzienda(t_rag_soc)
    ).selectExpr(RcuPodDistrSchema.getValues:_*)
      .as[RcuPodDistrModel]

    rcuPodDistr
  }

}
