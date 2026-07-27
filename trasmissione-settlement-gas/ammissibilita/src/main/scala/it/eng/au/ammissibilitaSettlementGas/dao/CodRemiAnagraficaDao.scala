package it.eng.au.ammissibilitaSettlementGas.dao

import it.eng.au.ammissibilitaSettlementGas.dao.`trait`.DAO
import it.eng.au.ammissibilitaSettlementGas.schema.{RcugasRemiPSchema, RcugasRemiStatoPSchema}
import it.eng.au.ammissibilitaSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.schema.TSGQKRIUDSchema
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, current_date, lit}
import org.apache.spark.sql.types.DateType

class CodRemiAnagraficaDao extends DAO{
  override val tableName: String = Properties.getRcugasRemiStatoPTableName
  override val columns: List[String] = RcugasRemiStatoPSchema.getValues
  override val partitionColumn: String = ""
  private val tableName2: String = Properties.getRcugasRemiTableName

  override def readTable: DataFrame = {
    val rcugasRemiStatoPDF = Environment.sqlContext.table(tableName).selectExpr(RcugasRemiStatoPSchema.getValues: _*)
    val rcugasRemiPDF = Environment.sqlContext.table(tableName2).selectExpr(RcugasRemiPSchema.getValues: _*)

    val remiAnagrafica = rcugasRemiStatoPDF
      .join(rcugasRemiPDF, Seq(RcugasRemiStatoPSchema.n_id_remi_anagrafica.toString), "inner")
      .filter(col(RcugasRemiStatoPSchema.t_cod_stato_tecnico) === "A")
      .filter(
        (current_date().cast(DateType) >= coalesce(col(RcugasRemiStatoPSchema.d_data_inizio).cast(DateType), lit("1999-01-01").cast(DateType))) &&
          (current_date().cast(DateType) <= coalesce(col(RcugasRemiStatoPSchema.d_data_fine).cast(DateType), lit("2059-01-01").cast(DateType)))
      )

    remiAnagrafica
      .withColumnRenamed(RcugasRemiPSchema.t_remi, TSGQKRIUDSchema.cod_remi)
      .select(TSGQKRIUDSchema.cod_remi)
      .distinct
  }
}
