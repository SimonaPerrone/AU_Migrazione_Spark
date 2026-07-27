package it.eng.au.portaleConsumi.dao.hive.cmg_gas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.flow.misure.MisureGasModel
import it.eng.au.portaleConsumi.model.hive.cmg_gas.PrtCmgTalPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.cmg_gas.PrtCmgTalPSchema
import it.eng.au.portaleConsumi.utility.common.Costanti.TAL
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DecimalType, IntegerType}

import java.sql.Timestamp

class PrtCmgTalPDao() extends HiveDao[PrtCmgTalPModel] {
  override val tableName: String = Environment.getProperty("hive.table.cmg_gas_prt_cmg_tal_p")
  override val schema: SchemaEnum = PrtCmgTalPSchema

  override def read(columns: List[String] = columns): Dataset[PrtCmgTalPModel] = {

    val spark = Environment.getSpark
    import spark.implicits._

    spark.sqlContext.read
      .table(tableName)
      .withColumn(PrtCmgTalPSchema.data_com_autolet_cf, to_date(col(PrtCmgTalPSchema.data_com_autolet_cf), "dd/MM/yyyy"))
      .withColumn(PrtCmgTalPSchema.let_tot_prel, col(PrtCmgTalPSchema.let_tot_prel).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[PrtCmgTalPModel]
  }

  def readNuoveMisure(limiteAnnoMese: String, ultimoCalcoloTs: Timestamp, dataCalcoloTs: Timestamp): Dataset[MisureGasModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    readTable()
      .where(col(PrtCmgTalPSchema.cod_pdr).isNotNull)
      .where(col(PrtCmgTalPSchema.annomese) >= limiteAnnoMese)
      .where(col(PrtCmgTalPSchema.annomese)  < "299912")
      .where(col(PrtCmgTalPSchema.d_caricamento) >= ultimoCalcoloTs)
      .where(col(PrtCmgTalPSchema.d_caricamento) < dataCalcoloTs)
      .withColumn(PrtCmgTalPSchema.data_com_autolet_cf, to_date(col(PrtCmgTalPSchema.data_com_autolet_cf), "dd/MM/yyyy"))
      .withColumn(PrtCmgTalPSchema.let_tot_prel, col(PrtCmgTalPSchema.let_tot_prel).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[PrtCmgTalPModel]
      .map(x => MisureGasModel(
        codice_pdr = x.cod_pdr,
        flusso = TAL,
        lettura = x.let_tot_prel,
        data_lettura = x.data_com_autolet_cf,
        motivazione = null,
        data_caricamento = x.d_caricamento,
        annomese = x.annomese
      ))
  }

}
