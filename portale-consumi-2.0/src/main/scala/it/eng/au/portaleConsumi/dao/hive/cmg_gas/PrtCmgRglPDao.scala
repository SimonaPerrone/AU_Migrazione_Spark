package it.eng.au.portaleConsumi.dao.hive.cmg_gas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.flow.misure.MisureGasModel
import it.eng.au.portaleConsumi.model.hive.cmg_gas.PrtCmgRglPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.cmg_gas.PrtCmgRglPSchema
import it.eng.au.portaleConsumi.utility.common.Costanti.RGL
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, concat, substring, to_date}
import org.apache.spark.sql.types.IntegerType

import java.sql.Timestamp

class PrtCmgRglPDao() extends HiveDao[PrtCmgRglPModel] {
  override val tableName: String = Environment.getProperty("hive.table.cmg_gas_prt_cmg_rgl_p")
  override val schema: SchemaEnum = PrtCmgRglPSchema

  override def read(columns: List[String]): Dataset[PrtCmgRglPModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    spark.sqlContext.read
      .table(tableName)
      .withColumn(PrtCmgRglPSchema.data_racc, to_date(col(PrtCmgRglPSchema.data_racc), "dd/MM/yyyy"))
      .withColumn(PrtCmgRglPSchema.let_tot_prel, col(PrtCmgRglPSchema.let_tot_prel).cast(IntegerType))
      // formato da MMyyyy a yyyyMM
      .withColumn(PrtCmgRglPSchema.mese_comp, concat(
        substring(col(PrtCmgRglPSchema.mese_comp), 3, 7), substring(col(PrtCmgRglPSchema.mese_comp), 1, 2)))
      .selectExpr(columns: _*)
      .as[PrtCmgRglPModel]
  }

  def readNuoveMisure(limiteAnnoMese: String, ultimoCalcoloTs: Timestamp, dataCalcoloTs: Timestamp): Dataset[MisureGasModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    readTable()
      .where(col(PrtCmgRglPSchema.cod_pdr).isNotNull)
      // formato da MMyyyy a yyyyMM
      .withColumn(PrtCmgRglPSchema.mese_comp, concat(
        substring(col(PrtCmgRglPSchema.mese_comp), 3, 7), substring(col(PrtCmgRglPSchema.mese_comp), 1, 2)))
      .where(col(PrtCmgRglPSchema.mese_comp) >= limiteAnnoMese) //data_racc in 1.0
      .where(col(PrtCmgRglPSchema.mese_comp) < "299912")
      .where(col(PrtCmgRglPSchema.d_caricamento) >= ultimoCalcoloTs)
      .where(col(PrtCmgRglPSchema.d_caricamento) < dataCalcoloTs)
      .where(col(PrtCmgRglPSchema.tipo_lettura) === "E")
      .withColumn(PrtCmgRglPSchema.data_racc, to_date(col(PrtCmgRglPSchema.data_racc), "dd/MM/yyyy"))
      .withColumn(PrtCmgRglPSchema.let_tot_prel, col(PrtCmgRglPSchema.let_tot_prel).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[PrtCmgRglPModel]
      .map(x => MisureGasModel(
        codice_pdr = x.cod_pdr,
        flusso = RGL,
        lettura = x.let_tot_prel,
        data_lettura = x.data_racc,
        motivazione = x.mot_rett_lett,
        data_caricamento = x.d_caricamento,
        annomese = x.mese_comp
      ))
  }
}
