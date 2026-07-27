package it.eng.au.portaleConsumi.dao.hive.cmg_gas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.flow.misure.MisureGasModel
import it.eng.au.portaleConsumi.model.hive.cmg_gas.PrtCmgTmlPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.cmg_gas.PrtCmgTmlPSchema
import it.eng.au.portaleConsumi.utility.common.Costanti.TML
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DecimalType, IntegerType}

import java.sql.Timestamp

class PrtCmgTmlPDao() extends HiveDao[PrtCmgTmlPModel] {
  override val tableName: String = Environment.getProperty("hive.table.cmg_gas_prt_cmg_tml_p")
  override val schema: SchemaEnum = PrtCmgTmlPSchema

  override def read(columns: List[String]): Dataset[PrtCmgTmlPModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    spark.sqlContext.read
      .table(tableName)
      .withColumn(PrtCmgTmlPSchema.data_racc, to_date(col(PrtCmgTmlPSchema.data_racc), "dd/MM/yyyy"))
      .withColumn(PrtCmgTmlPSchema.let_tot_prel, col(PrtCmgTmlPSchema.let_tot_prel).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[PrtCmgTmlPModel]
  }

  def readNuoveMisure(limiteAnnoMese: String, ultimoCalcoloTs: Timestamp, dataCalcoloTs: Timestamp): Dataset[MisureGasModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    readTable()
      .where(col(PrtCmgTmlPSchema.cod_pdr).isNotNull)
      .where(col(PrtCmgTmlPSchema.annomese) >= limiteAnnoMese)
      .where(col(PrtCmgTmlPSchema.annomese)  < "299912")
      .where(col(PrtCmgTmlPSchema.d_caricamento) >= ultimoCalcoloTs)
      .where(col(PrtCmgTmlPSchema.d_caricamento) < dataCalcoloTs)
      .where(col(PrtCmgTmlPSchema.tipo_lettura) === "E")
      .withColumn(PrtCmgTmlPSchema.data_racc, to_date(col(PrtCmgTmlPSchema.data_racc), "dd/MM/yyyy"))
      .withColumn(PrtCmgTmlPSchema.let_tot_prel, col(PrtCmgTmlPSchema.let_tot_prel).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[PrtCmgTmlPModel]
      .map(x => MisureGasModel(
        codice_pdr = x.cod_pdr,
        flusso = TML,
        lettura = x.let_tot_prel,
        data_lettura = x.data_racc,
        data_caricamento = x.d_caricamento,
        annomese = x.annomese
      ))
  }
}
