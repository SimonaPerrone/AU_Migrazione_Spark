package it.eng.au.portaleConsumi.dao.hive.cmg_gas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.flow.misure.MisureGasModel
import it.eng.au.portaleConsumi.model.hive.cmg_gas.PrtCmgTavPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.cmg_gas.{PrtCmgTalPSchema, PrtCmgTavPSchema}
import it.eng.au.portaleConsumi.utility.common.Costanti.TAV
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DecimalType, IntegerType}

import java.sql.Timestamp

class PrtCmgTavPDao() extends HiveDao[PrtCmgTavPModel] {
  override val tableName: String = Environment.getProperty("hive.table.cmg_gas_prt_cmg_tav_p")
  override val schema: SchemaEnum = PrtCmgTavPSchema

  override def read(columns: List[String] = columns): Dataset[PrtCmgTavPModel] = {

    val spark = Environment.getSpark
    import spark.implicits._

    spark.sqlContext.read
      .table(tableName)
      .withColumn(PrtCmgTavPSchema.data_com_autolet_cf, to_date(col(PrtCmgTavPSchema.data_com_autolet_cf), "dd/MM/yyyy"))
      .withColumn(PrtCmgTavPSchema.let_tot_prel, col(PrtCmgTavPSchema.let_tot_prel).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[PrtCmgTavPModel]
  }


  def readNuoveMisure(limiteAnnoMese: String, ultimoCalcoloTs: Timestamp, dataCalcoloTs: Timestamp): Dataset[MisureGasModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    readTable()
      .where(col(PrtCmgTavPSchema.cod_pdr).isNotNull)
      .where(col(PrtCmgTavPSchema.annomese) >= limiteAnnoMese)
      .where(col(PrtCmgTavPSchema.annomese)  < "299912")
      .where(col(PrtCmgTavPSchema.d_caricamento) >= ultimoCalcoloTs)
      .where(col(PrtCmgTavPSchema.d_caricamento) < dataCalcoloTs)
      .withColumn(PrtCmgTavPSchema.data_com_autolet_cf, to_date(col(PrtCmgTavPSchema.data_com_autolet_cf), "dd/MM/yyyy"))
      .withColumn(PrtCmgTavPSchema.let_tot_prel, col(PrtCmgTavPSchema.let_tot_prel).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[PrtCmgTavPModel]
      .map(x => MisureGasModel(
        codice_pdr = x.cod_pdr,
        flusso = TAV,
        lettura = x.let_tot_prel,
        data_lettura = x.data_com_autolet_cf,
        motivazione = null,
        data_caricamento = x.d_caricamento,
        annomese = x.annomese
      ))
  }

}
