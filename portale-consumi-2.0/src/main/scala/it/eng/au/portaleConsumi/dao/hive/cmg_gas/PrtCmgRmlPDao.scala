package it.eng.au.portaleConsumi.dao.hive.cmg_gas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.flow.misure.MisureGasModel
import it.eng.au.portaleConsumi.model.hive.cmg_gas.PrtCmgRmlPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.cmg_gas.PrtCmgRmlPSchema
import it.eng.au.portaleConsumi.utility.common.Costanti.RML
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.IntegerType

import java.sql.Timestamp

class PrtCmgRmlPDao() extends HiveDao[PrtCmgRmlPModel] {
  override val tableName: String = Environment.getProperty("hive.table.cmg_gas_prt_cmg_rml_p")
  override val schema: SchemaEnum = PrtCmgRmlPSchema

  override def read(columns: List[String]): Dataset[PrtCmgRmlPModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    spark.sqlContext.read
      .table(tableName)
      .withColumn(PrtCmgRmlPSchema.data_racc, to_date(col(PrtCmgRmlPSchema.data_racc), "dd/MM/yyyy"))
      .withColumn(PrtCmgRmlPSchema.let_tot_prel, col(PrtCmgRmlPSchema.let_tot_prel).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[PrtCmgRmlPModel]
  }

  def readNuoveMisure(limiteAnnoMese: String, ultimoCalcoloTs: Timestamp, dataCalcoloTs: Timestamp): Dataset[MisureGasModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    readTable()
      .where(col(PrtCmgRmlPSchema.cod_pdr).isNotNull)
      .where(col(PrtCmgRmlPSchema.annomese) >= limiteAnnoMese)
      .where(col(PrtCmgRmlPSchema.annomese)  < "299912")
      .where(col(PrtCmgRmlPSchema.d_caricamento) >= ultimoCalcoloTs)
      .where(col(PrtCmgRmlPSchema.d_caricamento) < dataCalcoloTs)
      .withColumn(PrtCmgRmlPSchema.data_racc, to_date(col(PrtCmgRmlPSchema.data_racc), "dd/MM/yyyy"))
      .withColumn(PrtCmgRmlPSchema.let_tot_prel, col(PrtCmgRmlPSchema.let_tot_prel).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[PrtCmgRmlPModel]
      .map(x => MisureGasModel(
        codice_pdr = x.cod_pdr,
        flusso = RML,
        lettura = x.let_tot_prel,
        data_lettura = x.data_racc,
        motivazione = x.mot_rett_lett,
        data_caricamento = x.d_caricamento,
        annomese = x.annomese
      ))
  }
}
