package it.eng.au.portaleConsumi.dao.hive.misuregas

import it.eng.au.portaleConsumi.dao.hive.HiveDao
import it.eng.au.portaleConsumi.model.hive.misuregas.CalcoloMisureGasModel
import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.schema.misuregas.CalcoloMisureGasSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

class CalcoloMisureGasDao() extends HiveDao[CalcoloMisureGasModel] {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)
  override val tableName: String = Environment.getProperty("hive.table.misure_data_calcolo")
  override val schema: SchemaEnum = CalcoloMisureGasSchema

  override def write(data: Dataset[CalcoloMisureGasModel], overwrite: Boolean): Unit = {
    super.write(data, overwrite = true)
  }

  /** *
   * Ritorna ultima data di calcolo presente su tabella
   * Se processo = null allora massima data calcolo in generale
   * se processo = 3M allora massima data per processo 3M
   * se processo = 33M allora massima data per processo 33M
   */
  def ultimaDataCalcolo(processo: String = null): Option[String] = {
    val optProcesso = Option(processo)
    try {
      val ds = optProcesso match {
        case Some(x) if x == "3M" => read().where(col(CalcoloMisureGasSchema.processo) === "3M")
        case Some(x) if x == "33M" => read().where(col(CalcoloMisureGasSchema.processo) === "33M")
        case None => read()
      }

      val result = ds.select(CalcoloMisureGasSchema.data_calcolo)
        .dropDuplicates()
        .orderBy(col(CalcoloMisureGasSchema.data_calcolo).desc_nulls_last)
        .take(1)

      result.length match {
        case 0 => None
        case _ => Option(result.head.getString(0))
      }
    } catch {
      case _: Throwable => None
    }
  }

  def aggiornaDataCalcolo(calcoloMisureGas: CalcoloMisureGasModel): Unit = {
    val spark = Environment.getSpark
    import spark.implicits._

    val nuovaRiga = Seq(calcoloMisureGas).toDS()

    val ds = read()
      .where(col(CalcoloMisureGasSchema.processo) =!= calcoloMisureGas.processo)
      .union(nuovaRiga)

    write(ds, overwrite = true)
  }

}
