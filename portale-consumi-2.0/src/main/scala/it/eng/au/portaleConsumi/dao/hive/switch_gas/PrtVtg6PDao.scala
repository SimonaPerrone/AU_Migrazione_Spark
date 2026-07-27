package it.eng.au.portaleConsumi.dao.hive.switch_gas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.flow.misure.MisureGasModel
import it.eng.au.portaleConsumi.model.hive.switch_gas.PrtVtg6PModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.switch_gas.PrtVtg6PSchema
import it.eng.au.portaleConsumi.utility.common.Costanti.VTG
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, date_format, to_date}
import org.apache.spark.sql.types.{DecimalType, IntegerType}

import java.sql.Timestamp
import java.text.SimpleDateFormat

class PrtVtg6PDao extends HiveDao[PrtVtg6PModel] {
  override val tableName: String = Environment.getProperty("hive.table.switch_gas_prt_vtg6_p")
  override val schema: SchemaEnum = PrtVtg6PSchema

  override def read(columns: List[String]): Dataset[PrtVtg6PModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    spark.sqlContext.read
      .table(tableName)
      .withColumn(PrtVtg6PSchema.d_data_mis_eff, to_date(col(PrtVtg6PSchema.d_data_mis_eff), "dd/MM/yyyy"))
      .withColumn(PrtVtg6PSchema.t_segn_mis_eff, col(PrtVtg6PSchema.t_segn_mis_eff).cast(IntegerType))
      .withColumn(PrtVtg6PSchema.t_segn_mis_sost, col(PrtVtg6PSchema.t_segn_mis_sost).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[PrtVtg6PModel]
  }


  def readNuoveMisure(limiteAnnoMese: String, ultimoCalcoloTs: Timestamp, dataCalcoloTs: Timestamp): Dataset[MisureGasModel] = {

    val spark = Environment.getSpark
    import spark.implicits._

    val annoMeseFormat = new SimpleDateFormat("yyyyMM")

    readTable()
      .where(col(PrtVtg6PSchema.t_codice_pdr).isNotNull)
      .where(date_format(col(PrtVtg6PSchema.d_data_mis_eff_ts), "yyyyMM") >= limiteAnnoMese)
      .where(date_format(col(PrtVtg6PSchema.d_data_mis_eff_ts), "yyyyMM") < "299912")
      .where(col(PrtVtg6PSchema.t_tipo_lettura) === "E")
      .where(col(PrtVtg6PSchema.d_caricamento) >= ultimoCalcoloTs)
      .where(col(PrtVtg6PSchema.d_caricamento) < dataCalcoloTs)
      .withColumn(PrtVtg6PSchema.d_data_mis_eff, to_date(col(PrtVtg6PSchema.d_data_mis_eff), "dd/MM/yyyy"))
      .withColumn(PrtVtg6PSchema.t_segn_mis_eff, col(PrtVtg6PSchema.t_segn_mis_eff).cast(IntegerType))
      .withColumn(PrtVtg6PSchema.t_segn_mis_sost, col(PrtVtg6PSchema.t_segn_mis_sost).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[PrtVtg6PModel]
      .map(x => MisureGasModel(
        codice_pdr = x.t_codice_pdr,
        lettura = if (x.t_segn_mis_eff == null) x.t_segn_mis_sost else x.t_segn_mis_eff,
        data_lettura = x.d_data_mis_eff_ts,
        data_caricamento = x.d_caricamento,
        annomese = annoMeseFormat.format(x.d_data_mis_eff_ts),
        flusso = VTG
      ))
  }
}
