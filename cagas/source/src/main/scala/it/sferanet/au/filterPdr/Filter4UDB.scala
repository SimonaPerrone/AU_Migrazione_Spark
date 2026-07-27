package it.sferanet.au.filterPdr

import it.sferanet.au.schema.{RcuAziendaPSchema, RcuGasBilanciamentoPSchema, RcuGasMassivoPSchema, RcuGasUdbPSchema}
import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{broadcast, col, current_timestamp}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class Filter4UDB extends Filter3UDD {
  override def getPdrs: RDD[String] = {
    val filePath = Environment.getFilterPdrUdbCsvPath

    val schema = StructType(Array(
      StructField("piva_udb", StringType)
    ))

    val udbDF = Environment.getSqlContext.read.format("csv").schema(schema).load(filePath)

    val rcuGasBilanciamentoP = getRcuGasBilanciamentoP.where(betweenDates(current_timestamp(), col(RcuGasBilanciamentoPSchema.d_data_inizio), col(RcuGasBilanciamentoPSchema.d_data_fine)))
    val rcuAziendaP = getRcuAziendaP
    val rcuGasUdbP = getRcuGasUdbP
    val rcuGasMassivoP = getRcuGasMassivoP
      .select(RcuGasMassivoPSchema.t_codice_pdr, RcuGasMassivoPSchema.n_id_pdr)
      .distinct()

    val pdrs = rcuAziendaP //get id_azienda for input pivas udb
      .join(broadcast(udbDF), col(RcuAziendaPSchema.t_piva) === col("piva_udb"), "inner")
      .drop(rcuAziendaP.col(RcuAziendaPSchema.t_piva))
      .select("piva_udb", RcuAziendaPSchema.n_id_azienda)
      //get id_udb from id_azienda
      .join(rcuGasUdbP, rcuGasUdbP(RcuGasUdbPSchema.n_id_azienda) === rcuAziendaP(RcuAziendaPSchema.n_id_azienda), "inner")
      .drop(rcuGasUdbP.col(RcuGasUdbPSchema.n_id_azienda))
      .select("piva_udb", RcuAziendaPSchema.n_id_azienda, RcuGasUdbPSchema.n_id_udb)
      //get cod_pdr from n_id_udb
      .join(rcuGasBilanciamentoP, rcuGasBilanciamentoP(RcuGasBilanciamentoPSchema.n_id_udb) === rcuGasUdbP(RcuGasUdbPSchema.n_id_udb), "inner")
      .drop(rcuGasBilanciamentoP.col(RcuGasBilanciamentoPSchema.n_id_udb))
      .select(RcuGasBilanciamentoPSchema.n_id_pdr)
      //get useful cod_pdr from id_pdr
      .join(rcuGasMassivoP, rcuGasBilanciamentoP(RcuGasMassivoPSchema.n_id_pdr) === rcuGasBilanciamentoP(RcuGasBilanciamentoPSchema.n_id_pdr))
      .drop(rcuGasMassivoP.col(RcuGasMassivoPSchema.n_id_pdr))
      .select(RcuGasMassivoPSchema.t_codice_pdr)
      .distinct()

    pdrs.rdd.map(_.getString(0)).intersection(super.getPdrs)
  }
}
