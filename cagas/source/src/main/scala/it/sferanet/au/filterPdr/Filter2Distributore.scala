package it.sferanet.au.filterPdr

import it.sferanet.au.schema.{RcuGasConnessioniDistr2PSchema, VRcuGasDistributorePSchema}
import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{broadcast, col, current_timestamp}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class Filter2Distributore extends FilterPdr {

  override def getPdrs: RDD[String] = {
    val filePath = Environment.getFilterPdrDistribuzioneCsvPath

    val schema = StructType(Array(
      StructField("distr", StringType)
    ))

    val distributoriDF = Environment.getSqlContext.read.format("csv").schema(schema).load(filePath)

    val rcuGasConnessioniDistr2p = getRcuGasConnessioniDistr2P
      .where(betweenDates(current_timestamp(), col(RcuGasConnessioniDistr2PSchema.d_data_inizio_conn), col(RcuGasConnessioniDistr2PSchema.d_data_fine_conn)))
      .select(
        RcuGasConnessioniDistr2PSchema.n_id_distr,
        RcuGasConnessioniDistr2PSchema.t_codice_pdr
      )

    val vRcuDistributoreP = getVRcuGasDistributoreP
      .select(
        VRcuGasDistributorePSchema.t_piva,
        VRcuGasDistributorePSchema.n_id_distributore
      )

    val pdrs = vRcuDistributoreP.join(broadcast(distributoriDF), vRcuDistributoreP(VRcuGasDistributorePSchema.t_piva) === distributoriDF("distr"))
      .join(rcuGasConnessioniDistr2p, vRcuDistributoreP(VRcuGasDistributorePSchema.n_id_distributore) === rcuGasConnessioniDistr2p(RcuGasConnessioniDistr2PSchema.n_id_distr))
      .select(rcuGasConnessioniDistr2p(RcuGasConnessioniDistr2PSchema.t_codice_pdr))
      .distinct()

    pdrs.rdd.map(_.getString(0))
  }
}
