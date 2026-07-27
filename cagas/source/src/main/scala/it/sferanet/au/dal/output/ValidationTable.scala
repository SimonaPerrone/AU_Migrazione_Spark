package it.sferanet.au.dal.output

import it.sferanet.au.model.{Flow, ReadTypeExtractorVisitor, Tds, Validation}
import it.sferanet.au.schema.ValidationSchema
import it.sferanet.au.utilities.Environment
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.Timestamp

class ValidationTable(session: String, executionId: Long) extends Serializable {

  private val outputPath = Environment.getValidationPath

  def createDataFrame(measure: RDD[Flow], tdsBc: Broadcast[scala.collection.Map[String, Tds]]): DataFrame = {
    val readTypeVisitor = new ReadTypeExtractorVisitor()
    val validationDF = Environment.getSqlContext.createDataFrame(
      measure.map(f => {
        val tds: Option[Tds] = tdsBc.value.get(f.pdr)

        Validation(service = f.service,
          pdr = f.pdr,
          dat = new Timestamp(f.date.get.getTime),
          measure = f.measure,
          converted = f.converted,
          readtype = f.accept(readTypeVisitor),
          serialnumbermis = f.serialNumberMis,
          serialnumberconv = f.serialNumberConv,
          timestamplocalfile = new Timestamp(f.timestampLocalFile.getTime),
          d_caricamento = f.dateLoadFromLocalFile,
          local_file = f.local_file,
          cat_uso = if (tds.isDefined) tds.get.cat_uso else null,
          classe_prelievo = if (tds.isDefined) tds.get.classe_prelievo else null,
          data_creazione = if (tds.isDefined) new Timestamp(tds.get.data_creazione.getTime) else null,
          motivazione_rettifica = f.motivation,
          cau_int_mis = Flow.getCauIntMis(f),
          cau_int_cor = Flow.getCauIntCor(f),
          file_rettifica = f.fileRettifica,
          n_coeff_correzione = f.coefCorr,
          session = session,
          executionid = executionId
        )
      }))

    validationDF
  }


  def write(df: DataFrame): Unit = {
    df
      .selectExpr(ValidationSchema.getValues: _*)
      .write
      .partitionBy(ValidationSchema.session, ValidationSchema.executionid)
      .mode(SaveMode.Append)
      .parquet(outputPath)

    if(!Environment.isLocalMode) Environment.getSpark.sql(s"MSCK REPAIR TABLE ${Environment.getValidationTable}")
  }
}

