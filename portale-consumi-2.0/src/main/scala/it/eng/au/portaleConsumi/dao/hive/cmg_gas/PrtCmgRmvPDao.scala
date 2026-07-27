package it.eng.au.portaleConsumi.dao.hive.cmg_gas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.flow.misure.MisureGasModel
import it.eng.au.portaleConsumi.model.hive.cmg_gas.PrtCmgRmvPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.cmg_gas.PrtCmgRmvPSchema
import it.eng.au.portaleConsumi.utility.common.Costanti.RMV
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.IntegerType

import java.sql.Timestamp

class PrtCmgRmvPDao() extends HiveDao[PrtCmgRmvPModel] {
  override val tableName: String = Environment.getProperty("hive.table.cmg_gas_prt_cmg_rmv_p")
  override val schema: SchemaEnum = PrtCmgRmvPSchema

  override def read(columns: List[String]): Dataset[PrtCmgRmvPModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    spark.sqlContext.read
      .table(tableName)
      .withColumn(PrtCmgRmvPSchema.data_racc, to_date(col(PrtCmgRmvPSchema.data_racc), "dd/MM/yyyy"))
      .withColumn(PrtCmgRmvPSchema.let_tot_prel, col(PrtCmgRmvPSchema.let_tot_prel).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[PrtCmgRmvPModel]
  }

  def readNuoveMisure(limiteAnnoMese: String, ultimoCalcoloTs: Timestamp, dataCalcoloTs: Timestamp): Dataset[MisureGasModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    readTable()
      .where(col(PrtCmgRmvPSchema.cod_pdr).isNotNull)
      .where(col(PrtCmgRmvPSchema.annomese) >= limiteAnnoMese)
      .where(col(PrtCmgRmvPSchema.annomese)  < "299912")
      .where(col(PrtCmgRmvPSchema.d_caricamento) >= ultimoCalcoloTs)
      .where(col(PrtCmgRmvPSchema.d_caricamento) < dataCalcoloTs)
      .withColumn(PrtCmgRmvPSchema.data_racc, to_date(col(PrtCmgRmvPSchema.data_racc), "dd/MM/yyyy"))
      .withColumn(PrtCmgRmvPSchema.let_tot_prel, col(PrtCmgRmvPSchema.let_tot_prel).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[PrtCmgRmvPModel]
      .map(x => MisureGasModel(
        codice_pdr = x.cod_pdr,
        flusso = RMV,
        lettura = x.let_tot_prel,
        data_lettura = x.data_racc,
        motivazione = x.mot_rett_lett,
        data_caricamento = x.d_caricamento,
        annomese = x.annomese // annomese_riferimento in 1.0
      ))
  }

}
