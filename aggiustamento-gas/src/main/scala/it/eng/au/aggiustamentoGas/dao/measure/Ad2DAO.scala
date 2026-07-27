package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.model.measure.{Ad2, Flow}
import it.eng.au.aggiustamentoGas.schema.measure.Ad2Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

class Ad2DAO extends MeasureDAO {
  override val parquetPath: String = Environment.getAd2ParquetPath
  override val partitionDateColumn: Column = MeasureDAO.annoMeseColumn
  override val columns: List[String] = List(
    Ad2Schema.cod_flusso,
    Ad2Schema.cod_pdr,
    Ad2Schema.data_prest,
    Ad2Schema.let_tot_prel,
    Ad2Schema.let_tot_conv,
    Ad2Schema.matr_mis,
    Ad2Schema.matr_conv,
    Ad2Schema.local_file,
    Ad2Schema.d_caricamento,
    Ad2Schema.piva_distr,
    Ad2Schema.piva_utente,
    Ad2Schema.tipo_lettura,
    MeasureDAO.ANNO_MESE_COL_NAME
  )

  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))

  override val mapFunc: Row => Flow = (r: Row) => {
    Ad2(
      service = r.getAs[String](Ad2Schema.cod_flusso).toUpperCase,
      pdr = r.getAs[String](Ad2Schema.cod_pdr),
      date = MeasureDAO.parseDateToOption(r.getAs[String](Ad2Schema.data_prest)),
      readType = Try(Option(r.getAs[String](Ad2Schema.tipo_lettura)).map(_.trim.charAt(0))).getOrElse(None),
      measure = Try(Option(r.getAs[String](Ad2Schema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(r.getAs[String](Ad2Schema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(r.getAs[String](Ad2Schema.matr_mis)),
      serialNumberConv = Option(r.getAs[String](Ad2Schema.matr_conv)),
      localFile = Option(r.getAs[String](Ad2Schema.local_file)),
      pivaDistr = Option(r.getAs[String](Ad2Schema.piva_distr)),
      pivaUtente = Option(r.getAs[String](Ad2Schema.piva_utente)),
      dataCaricamento = MeasureDAO.parseDateToOption(r.getAs[String](Ad2Schema.d_caricamento), MeasureDAO.dateLoadFormatter)
    )
  }
}
