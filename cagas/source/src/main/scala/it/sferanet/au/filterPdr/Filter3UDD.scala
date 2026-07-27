package it.sferanet.au.filterPdr

import it.sferanet.au.schema.RcuGasMassivoPSchema
import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{broadcast, col, current_timestamp}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class Filter3UDD extends Filter2Distributore {
  override def getPdrs: RDD[String] = {
    val filePath = Environment.getFilterPdrUddCsvPath

    val schema = StructType(Array(
      StructField("udd", StringType)
    ))

    val uddDF = Environment.getSqlContext.read.format("csv").schema(schema).load(filePath)

    val rcuGasMassivoP = getRcuGasMassivoP
      .where(betweenDates(current_timestamp(), col(RcuGasMassivoPSchema.d_data_inizio_for), col(RcuGasMassivoPSchema.data_fine_for)) &&
        col(RcuGasMassivoPSchema.n_id_fornitura).isNotNull)
      .select(
        RcuGasMassivoPSchema.t_codice_pdr,
        RcuGasMassivoPSchema.piva_udd
      )

    val pdrs = rcuGasMassivoP.join(broadcast(uddDF), col(RcuGasMassivoPSchema.piva_udd) === col("udd")).select(RcuGasMassivoPSchema.t_codice_pdr).distinct

    pdrs.rdd.map(_.getString(0)).intersection(super.getPdrs)
  }
}
