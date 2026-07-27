package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Ad2r, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.Ad2rSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class Ad2rDAO extends MeasureDAO {
  override val parquetPath: String = Environment.getAd2rParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    Ad2rSchema.cod_flusso,
    Ad2rSchema.cod_pdr,
    Ad2rSchema.data_prest,
    Ad2rSchema.let_tot_prel,
    Ad2rSchema.let_tot_conv,
    Ad2rSchema.matr_mis,
    Ad2rSchema.matr_conv,
    Ad2rSchema.local_file,
    Ad2rSchema.d_caricamento,
    Ad2rSchema.piva_distr,
    Ad2rSchema.piva_utente,
    Ad2rSchema.mot_ret_lett,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5).contains(f.motivation.getOrElse(-1))

  override val mapFunc: Row => Flow = (r: Row) => {
    Ad2r(
      service = r.getAs[String](Ad2rSchema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](Ad2rSchema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](Ad2rSchema.data_prest)),
      measure = Try(Option(r.getAs[String](Ad2rSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](Ad2rSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](Ad2rSchema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](Ad2rSchema.matr_conv)),
      localFile = Option(r.getAs[String](Ad2rSchema.local_file)),
      pivaDistr = Option(r.getAs[String](Ad2rSchema.piva_distr)),
      pivaUtente = Option(r.getAs[String](Ad2rSchema.piva_utente)),
      motivation = Try(Option(r.getAs[String](Ad2rSchema.mot_ret_lett).toInt)).getOrElse(None),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](Ad2rSchema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}
