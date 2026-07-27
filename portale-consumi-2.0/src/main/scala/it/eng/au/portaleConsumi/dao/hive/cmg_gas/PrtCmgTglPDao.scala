package it.eng.au.portaleConsumi.dao.hive.cmg_gas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.flow.misure.MisureGasModel
import it.eng.au.portaleConsumi.model.hive.cmg_gas.PrtCmgTglPModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.cmg_gas.{PrtCmgTavPSchema, PrtCmgTglPSchema}
import it.eng.au.portaleConsumi.utility.common.Costanti.TGL
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, Dataset, functions}
import org.apache.spark.sql.functions.{col, concat, date_format, substring, to_date}
import org.apache.spark.sql.types.{DecimalType, IntegerType}

import java.sql.Timestamp

class PrtCmgTglPDao() extends HiveDao[PrtCmgTglPModel] {
  override val tableName: String = Environment.getProperty("hive.table.cmg_gas_prt_cmg_tgl_p")
  override val schema: SchemaEnum = PrtCmgTglPSchema

  override def read(columns: List[String] = columns): Dataset[PrtCmgTglPModel] = {

    val spark = Environment.getSpark
    import spark.implicits._

    spark.sqlContext.read
      .table(tableName)
      .withColumn(PrtCmgTglPSchema.data_racc, to_date(col(PrtCmgTglPSchema.data_racc), "dd/MM/yyyy"))
      .withColumn(PrtCmgTglPSchema.data_comp, to_date(col(PrtCmgTglPSchema.data_comp), "dd/MM/yyyy"))
      // formato da MMyyyy a yyyyMM
      .withColumn(PrtCmgTglPSchema.mese_comp, concat(
        substring(col(PrtCmgTglPSchema.mese_comp), 3, 7), substring(col(PrtCmgTglPSchema.mese_comp), 1, 2)))
      .withColumn(PrtCmgTglPSchema.let_tot_prel, col(PrtCmgTglPSchema.let_tot_prel).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[PrtCmgTglPModel]
  }


  def readNuoveMisure(limiteAnnoMese: String, ultimoCalcoloTs: Timestamp, dataCalcoloTs: Timestamp): Dataset[MisureGasModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    readTable()
      .where(col(PrtCmgTglPSchema.cod_pdr).isNotNull)
      // formato da MMyyyy a yyyyMM
      .withColumn(PrtCmgTglPSchema.mese_comp, concat(
        substring(col(PrtCmgTglPSchema.mese_comp), 3, 7), substring(col(PrtCmgTglPSchema.mese_comp), 1, 2)))
      .where(col(PrtCmgTglPSchema.mese_comp) >= limiteAnnoMese)
      .where(col(PrtCmgTglPSchema.mese_comp) < "299912")
      .where(col(PrtCmgTglPSchema.d_caricamento) >= ultimoCalcoloTs)
      .where(col(PrtCmgTglPSchema.d_caricamento) < dataCalcoloTs)
      .where(col(PrtCmgTglPSchema.tipo_lettura) === "E")
      .withColumn(PrtCmgTglPSchema.data_racc, to_date(col(PrtCmgTglPSchema.data_racc), "dd/MM/yyyy"))
      .withColumn(PrtCmgTglPSchema.data_comp, to_date(col(PrtCmgTglPSchema.data_comp), "dd/MM/yyyy"))
      .withColumn(PrtCmgTglPSchema.let_tot_prel, col(PrtCmgTglPSchema.let_tot_prel).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[PrtCmgTglPModel]
      .map(x => MisureGasModel(
        codice_pdr = x.cod_pdr,
        flusso = TGL,
        lettura = x.let_tot_prel,
        data_lettura = x.data_comp,
        motivazione = null,
        data_caricamento = x.d_caricamento,
        annomese = x.mese_comp
      ))
  }

}
