package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.{ANNO_MESE_COL_NAME, annoMeseColumn, isEmptyOrNull}
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Fui}
import it.eng.au.aggiustamentoGas.schema.measure.FuiSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class FuiDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getFuiParquetPath
  override val partitionDateColumn: Column = annoMeseColumn
  override val columns: List[String] = List(
    FuiSchema.cod_servizio,
    FuiSchema.cod_pdr,
    FuiSchema.data_prest,
    FuiSchema.tipo_lettura,
    FuiSchema.let_tot_prel,
    FuiSchema.let_tot_conv,
    FuiSchema.matr_mis,
    FuiSchema.matr_conv,
    FuiSchema.local_file,
    FuiSchema.d_caricamento,
    FuiSchema.piva_distr,
    FuiSchema.piva_utente,
    ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    val dataPrest = r.getAs[String](FuiSchema.data_prest)
    val letTotPrel = r.getAs[String](FuiSchema.let_tot_prel)
    val letTotConv = r.getAs[String](FuiSchema.let_tot_conv)

    Fui(
      service = r.getAs[String](FuiSchema.cod_servizio).toUpperCase,
      pdr = r.getAs[String](FuiSchema.cod_pdr),
      readType = Try(Option(r.getAs[String](FuiSchema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      date = MeasureDAO.parseDateToOption(dataPrest),
      measure = Try(Option(letTotPrel).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(letTotConv).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](FuiSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](FuiSchema.matr_conv)),
      localFile = Option(r.getAs[String](FuiSchema.local_file)),
      pivaDistr = Option(r.getAs[String](FuiSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](FuiSchema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](FuiSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}
